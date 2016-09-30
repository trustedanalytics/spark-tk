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

import org.trustedanalytics.sparktk.frame.Frame
import org.apache.spark.sql.functions.{ sum, array, col, count, explode, struct }
import org.graphframes.GraphFrame
import org.apache.spark.sql.DataFrame
import org.graphframes.lib.AggregateMessages

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

import org.trustedanalytics.sparktk.graph.internal.{ GraphState, GraphSummarization, BaseGraph }

trait ConnectedComponentsSummarization extends BaseGraph {
  /**
   *
   * Connected components are disjoint subgraphs in which all vertices are
   * connected to all other vertices in the same component via paths, but not
   * connected via paths to vertices in any other component.
   *
   * @return The dataframe containing the vertices and which component they belong to
   */
  def connectedComponents(): Frame = {
    execute[Frame](ConnectedComponents())
  }
}

case class ConnectedComponents() extends GraphSummarization[Frame] {

  override def work(state: GraphState): Frame = {
    new Frame(state.graphFrame.connectedComponents.run.toDF("Vertex", "Component"))
  }
}
