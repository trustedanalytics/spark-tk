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
import org.trustedanalytics.sparktk.graph.internal.{BaseGraph, GraphSummarization, GraphState}
import org.graphframes.lib.org.trustedanalytics.{sparktk => graphframeslib}

trait SingleSourceShortestPathSummarization extends BaseGraph {

  /**
    * Computes the Single Source Shortest Paths (SSSP)for the given graph starting from the given vertex ID,
    * it returns the target vertexID, he shortest path from the source vertex and the corresponding cost.
    * It utilizes a distributed version of Dijkstra-based shortest path algorithm. Some optional parameters, e.g., maximum path length, are provided
    * to constraint the computations for large graphs.
    *
    * @param srcVertexId source vertex ID
    * @param edgePropName optional edge column name to be used as edge weight
    * @param maxPathLength optional maximum path length or cost to limit the SSSP computations
    * @return vertices dataframe with two additional nodes for the shortest path and the cost
    */
  def singleSourceShortestPath(srcVertexId: Any,
                               edgePropName: Option[String] = None,
                               maxPathLength: Option[Double] = None): Frame = {
    execute[Frame](SingleSourceShortestPath(srcVertexId,edgePropName,maxPathLength))
  }
}
case class SingleSourceShortestPath(srcVertexId: Any,
                                    edgePropName: Option[String] = None,
                                    maxPathLength: Option[Double] = None) extends GraphSummarization[Frame] {

  override def work(state: GraphState): Frame ={
    new Frame(graphframeslib.SingleSourceShortestPath.run(state.graphFrame,srcVertexId,edgePropName,maxPathLength))
  }
}

