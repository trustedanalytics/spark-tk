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
package org.graphframes.lib.org.trustedanalytics.sparktk

import org.apache.spark.graphx.lib.org.trustedanalytics._
import org.apache.spark.graphx.lib.org.trustedanalytics.sparktk.ClosenessCalculations
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.graphframes.GraphFrame

/**
 * Compute closeness centrality for nodes.
 *
 * Closeness centrality of a node is the reciprocal of the sum of the shortest path distances from this node to all
 * other nodes in the graph. Since the sum of distances depends on the number of nodes in the
 * graph, closeness is normalized by the sum of minimum possible distances.
 *
 * In the case of disconnected graph, the algorithm computes the closeness centrality for each connected part.
 *
 * If the edge weight is considered then the shortest-path length will be computed using Dijkstra's algorithm with
 * that edge weight.
 *
 * Reference: Linton C. Freeman: Centrality in networks: I.Conceptual clarification. Social Networks 1:215-239, 1979.
 * http://leonidzhukov.ru/hse/2013/socialnetworks/papers/freeman79-centrality.pdf
 */
object ClosenessCentrality {

  /**
   * Compute the closeness centrality
   *
   * @param graph the graph to compute the closeness centrality for its nodes
   * @param edgePropName optional edge column name to be used as edge weight
   * @param normalized normalizes the closeness centrality value to the number of nodes connected to it divided by
   *                   the rest number of nodes in the graph, this is effective in the case of disconnected graph
   * @return the graph vertex IDs and each corresponding closeness centrality value
   */
  def run(graph: GraphFrame, edgePropName: Option[String] = None, normalized: Boolean = true): Seq[ClosenessCalculations] = {
    val edgeWeightFunc: Option[(Row) => Double] = getEdgeWeightFunc(graph, edgePropName)
    sparktk.ClosenessCentrality.run(graph.toGraphX, edgeWeightFunc, normalized)
  }

  /**
   * Get the edge weight function that enables the inclusion of the edge weights in the shortest-path
   * calculations by converting the edge attribute type to Double
   *
   * @param graph graph to compute shortest-paths against
   * @param edgePropName column name for the edge weight
   * @return edge weight function
   */
  def getEdgeWeightFunc(graph: GraphFrame, edgePropName: Option[String]): Option[(Row) => Double] = {
    val edgeWeightFunc = if (edgePropName.isDefined) {
      val edgeWeightType = graph.edges.schema(edgePropName.get).dataType
      require(edgeWeightType.isInstanceOf[NumericType], "The edge weight type should be numeric")
      Some((row: Row) => row.getAs[Any](edgePropName.get) match {
        case x: Int => x.toDouble
        case x: Long => x.toDouble
        case x: Short => x.toDouble
        case x: Byte => x.toDouble
        case x: Double => x
        case _ => throw new scala.ClassCastException(s"the edge weight type cannot be $edgeWeightType")
      })
    }
    else {
      None
    }
    edgeWeightFunc
  }
}
