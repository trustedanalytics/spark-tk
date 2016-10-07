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

import org.apache.spark.sql.Row
import org.trustedanalytics.sparktk.frame.internal._
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd

trait FilterTransform extends BaseFrame {
  /**
   * Select all rows which satisfy a predicate.
   *
   * Modifies the current frame to save defined rows and delete everything else.
   *
   * @note
   *       1. The rowFunction must return a boolean
   *
   * @param rowFunction map function which decides whether to keep a row or drop
   */
  def filter(rowFunction: Row => Boolean): Unit = {
    execute(Filter(rowFunction))
  }

}

case class Filter(rowFunction: Row => Boolean) extends FrameTransform {

  require(rowFunction != null, "predicate is required")

  override def work(state: FrameState): FrameState = {
    val filterRdd = (state: FrameRdd).filter(rowFunction)
    FrameState(filterRdd, state.schema)
  }
}