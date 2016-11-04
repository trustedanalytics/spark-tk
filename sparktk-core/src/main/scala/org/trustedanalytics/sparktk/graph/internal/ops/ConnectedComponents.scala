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
import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame
import org.graphframes.GraphFrame.ID
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

  val index = "component"
  val indexNew = "componentNew"

  override def work(state: GraphState): Frame = {

    var graph = GraphFrame(state.graphFrame.vertices.withColumn(index, monotonicallyIncreasingId()), state.graphFrame.edges)
    var continue = true
    var getMin = udf { (a: Long, b: Long) => if (a > b) b else a }

    while (continue) {
      val updatedComponent = graph
        .aggregateMessages
        .sendToDst(getMin(AggregateMessages.src(index), AggregateMessages.dst(index)))
        .sendToSrc(getMin(AggregateMessages.src(index), AggregateMessages.dst(index)))
        .agg(min(AggregateMessages.msg).as(indexNew))

      val joinedComponent = updatedComponent
        .join(graph.vertices, graph.vertices(ID) === updatedComponent(ID))
        .drop(graph.vertices(ID))

      // TODO: switch this to an accumulator argument
      continue = joinedComponent.where(col(index) !== col(indexNew)).count() > 0

      val newVertices = joinedComponent
        .drop(index)
        .withColumnRenamed(indexNew, index)

      val unCachedVertices = AggregateMessages.getCachedDataFrame(newVertices)

      graph = GraphFrame(unCachedVertices, state.graphFrame.edges)
    }

    new Frame(graph.vertices)
  }
}
