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
import org.trustedanalytics.sparktk.graph.internal.{ BaseGraph, GraphSummarization, GraphState }
import org.graphframes.lib.org.trustedanalytics.{ sparktk => graphframeslib }

trait SingleSourceShortestPathSummarization extends BaseGraph {

  /**
   * Computes the Single Source Shortest Path (SSSP) for the graph starting from the given vertex ID to every vertex in the graph.
   * The algorithm returns the shortest path from the source vertex and the corresponding cost.
   * This implementation utilizes a distributed version of Dijkstra's shortest path algorithm.
   * Some optional parameters, e.g., maximum path length, constrain the computations for large graphs.
   *
   * @param srcVertexId the source vertex ID
   * @param edgeWeight the name of the column containing the edge weights. If none, every edge is assigned a weight of 1
   * @param maxPathLength optional maximum path length or cost to limit the SSSP computations
   * @return frame with the shortest path and corresponding cost from the source vertex to each target vertex ID.
   */
  def singleSourceShortestPath(srcVertexId: Any,
                               edgeWeight: Option[String] = None,
                               maxPathLength: Option[Double] = None): Frame = {
    execute[Frame](SingleSourceShortestPath(srcVertexId, edgeWeight, maxPathLength))
  }
}
case class SingleSourceShortestPath(srcVertexId: Any,
                                    edgeWeight: Option[String] = None,
                                    maxPathLength: Option[Double] = None) extends GraphSummarization[Frame] {

  override def work(state: GraphState): Frame = {
    new Frame(graphframeslib.SingleSourceShortestPath.run(state.graphFrame, srcVertexId, edgeWeight, maxPathLength))
  }
}

