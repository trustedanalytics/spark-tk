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
import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameSummarization, BaseFrame }
import org.apache.spark.sql.Row

trait CountSummarization extends BaseFrame {
  /**
   * Count the number of rows which meet the given criteria specified by a predicate.
   *
   * @param whereFunction Evaluates the row to a boolean
   * @return Number of rows matching qualifications.
   */
  def count(whereFunction: Row => Boolean): Long = {
    execute(Count(whereFunction))
  }
}

case class Count(whereFunction: Row => Boolean) extends FrameSummarization[Long] {
  require(whereFunction != null, "where predicate is required")
  override def work(state: FrameState): Long = {
    (state: FrameRdd).filter(whereFunction).count()
  }
}

