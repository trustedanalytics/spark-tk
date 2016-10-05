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

import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameTransform, BaseFrame }

trait RenameColumnsTransform extends BaseFrame {
  /**
   * Rename columns
   *
   * @param names Map of old names to new names.
   */
  def renameColumns(names: Map[String, String]): Unit = {
    execute(RenameColumns(names))
  }
}

case class RenameColumns(names: Map[String, String]) extends FrameTransform {
  require(names != null, "names parameter is required.")

  override def work(state: FrameState): FrameState = {
    val schema = state.schema.renameColumns(names)
    FrameState(state.rdd, schema)
  }
}