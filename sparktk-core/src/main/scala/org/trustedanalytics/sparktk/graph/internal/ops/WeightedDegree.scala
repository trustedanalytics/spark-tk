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

trait WeightedDegreeSummarization extends BaseGraph {
  /**
   * Returns a frame with annotations concerning the degree of each node,
   * weighted by a given edge weight
   *
   * @param edgeWeight the name of the weight value in the edge
   * @param degreeOption One of "in", "out" or "undirected". Determines the edge direction for the degree
   * @param defaultWeight A weight value to use if there is no entry for the weight property on an edge
   * @return The dataframe containing the vertices and their corresponding weights
   */
  def weightedDegree(edgeWeight: String, degreeOption: String = "undirected", defaultWeight: Float = 0.0f): Frame = {
    execute[Frame](WeightedDegree(edgeWeight, degreeOption, defaultWeight))
  }
}

case class WeightedDegree(edgeWeight: String, degreeOption: String, defaultWeight: Float) extends GraphSummarization[Frame] {
  require(degreeOption == "in" || degreeOption == "out" || degreeOption == "undirected", "Invalid degree option, please choose \"in\", \"out\", or \"undirected\"")

  val outputName = "degree"

  override def work(state: GraphState): Frame = {
    require(state.graphFrame.edges.columns.contains(edgeWeight), s"Property $edgeWeight not found")

    val graphFrame = GraphFrame(state.graphFrame.vertices, state.graphFrame.edges.na.fill(defaultWeight, List(edgeWeight)))
    // If you are counting the in Degrees you are looking at the destination of
    // the edges, if you are looking at the out degrees you are looking at the
    // source
    val (dstMsg, srcMsg) = degreeOption match {
      case "in" => (AggregateMessages.edge(edgeWeight), lit(0))
      case "out" => (lit(0), AggregateMessages.edge(edgeWeight))
      case "undirected" => (AggregateMessages.edge(edgeWeight), AggregateMessages.edge(edgeWeight))
    }
    val weightedDegrees = graphFrame.aggregateMessages
      .sendToDst(dstMsg)
      .sendToSrc(srcMsg)
      .agg(sum(AggregateMessages.msg).as(outputName))

    // Join the isolated vertices into the return frame, set their degree to 0
    // This is working around an anomolous property of the message sending system that
    // isolated vertices(i.e. vertices with no edges) can not be sent, or send, messages
    // which causes them to be dropped from the frame
    val degreesJoinedWithIsolatedVertices = state.graphFrame.vertices
      .join(weightedDegrees, state.graphFrame.vertices(GraphFrame.ID).equalTo(weightedDegrees(GraphFrame.ID)), "left")
      .drop(weightedDegrees(GraphFrame.ID))
      .na.fill(0.0, Array(outputName))
    new Frame(degreesJoinedWithIsolatedVertices)
  }
}
