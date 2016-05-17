/**
 *  Copyright (c) 2015 Intel Corporation 
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.trustedanalytics.sparktk.frame.internal.ops.sortedk

import org.trustedanalytics.sparktk.frame.Frame
import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameSummarization, BaseFrame }

trait SortedKSummarization extends BaseFrame {

  def sortedK(k: Int,
              columnNamesAndAscending: List[(String, Boolean)],
              reduceTreeDepth: Int = 2): Frame = {
    execute(SortedK(k, columnNamesAndAscending, reduceTreeDepth))
  }
}

/**
 * Get a sorted subset of data.
 *
 * @param k Number of sorted records to return.
 * @param columnNamesAndAscending Column names to sort by, and true to sort column by ascending order,
 *                                or false for descending order.
 * @param reduceTreeDepth Advanced tuning parameter which determines the depth of the reduce-tree
 *                        (uses Spark's treeReduce() for scalability.)
 *                        Default is 2.
 */
case class SortedK(k: Int,
                   columnNamesAndAscending: List[(String, Boolean)],
                   reduceTreeDepth: Int) extends FrameSummarization[Frame] {

  require(k > 0, "k should be greater than zero") //TODO: Should we add an upper bound for K
  require(columnNamesAndAscending != null && columnNamesAndAscending.nonEmpty, "one or more columnNames is required")
  require(reduceTreeDepth >= 1, s"Depth of reduce tree must be greater than or equal to 1")

  override def work(state: FrameState): Frame = {
    // return new frame with top-k sorted records
    val sortedKFrame = SortedKFunctions.takeOrdered(
      state,
      k,
      columnNamesAndAscending,
      reduceTreeDepth
    )

    new Frame(sortedKFrame.rdd, sortedKFrame.schema)
  }

}

