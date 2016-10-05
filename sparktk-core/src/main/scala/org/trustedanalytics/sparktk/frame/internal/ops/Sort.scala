/**
 *  Copyright (c) 2016 Intel Corporation 
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
package org.trustedanalytics.sparktk.frame.internal.ops

import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameTransform, BaseFrame }

trait SortTransform extends BaseFrame {
  /**
   * Sort by one or more columns.
   *
   * @param columnNamesAndAscending Column names to sort by, true for ascending, false for descending.
   */
  def sort(columnNamesAndAscending: List[(String, Boolean)]): Unit = {
    execute(Sort(columnNamesAndAscending))
  }
}

case class Sort(columnNamesAndAscending: List[(String, Boolean)]) extends FrameTransform {
  require(columnNamesAndAscending != null && columnNamesAndAscending.nonEmpty, "one or more column names is required.")
  override def work(state: FrameState): FrameState = {
    (state: FrameRdd).sortByColumns(columnNamesAndAscending)
  }
}
