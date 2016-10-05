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
package org.trustedanalytics.sparktk.graph.internal.ops

import org.trustedanalytics.sparktk.graph.Graph
import org.trustedanalytics.sparktk.graph.internal.{ GraphState, GraphSummarization, BaseGraph }
import org.trustedanalytics.sparktk.saveload.TkSaveLoad

trait SaveSummarization extends BaseGraph {
  /**
   * Save the current frame.
   *
   * @param path The destination path.
   */
  def save(path: String): Unit = {
    execute(Save(path))
  }
}

case class Save(path: String) extends GraphSummarization[Unit] {

  override def work(state: GraphState): Unit = {
    state.graphFrame.vertices.write.parquet(path + "/vertices")
    state.graphFrame.edges.write.parquet(path + "/edges")
    val formatId = Graph.formatId
    val formatVersion = Graph.tkFormatVersion
    TkSaveLoad.saveTk(state.graphFrame.vertices.sqlContext.sparkContext, path, formatId, formatVersion, "No Metadata")
  }
}
