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
package org.apache.spark.graphx.lib.org.trustedanalytics.sparktk

import org.apache.spark.graphx._
import scala.reflect.ClassTag

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
   * computes the closeness centrality
   *
   * @param graph the graph to compute the closeness centrality for its nodes
   * @param getEdgeWeight optional user-defined function that enables the inclusion of the edge weights in the
   *                      shortest-path calculations by converting the edge attribute type to Double.
   * @param normalized if true, normalizes the closeness centrality value to the number of nodes connected to it
   *                   divided by the total number of nodes in the graph, this is effective in the case of disconnected
   *                   graph
   * @tparam VD the vertex attribute that stores the closeness centrality data
   * @tparam ED the edge attribute that contains the edge weight
   * @return graph with the closeness data as the vertex attribute and the edge weight as the edge attribute
   */
  def run[VD, ED: ClassTag](graph: Graph[VD, ED],
                            getEdgeWeight: Option[ED => Double] = None,
                            normalized: Boolean = true): Graph[Double, Double] = {
    val verticesCount = graph.vertices.count.toDouble
    calculateShortestPaths(graph, getEdgeWeight).mapVertices {
      case (id, spMap) => calculateCloseness(id, spMap, normalized, verticesCount).closenessCentrality
    }
  }

  /**
   * calculate the closeness centrality value per vertex
   *
   * @param id the vertex ID
   * @param spMap the shortest-path calculations from that vertex to the rest of the nodes in the graph
   * @param normalized if true, normalizes the closeness centrality value to the number of nodes connected to it
   *                   divided by the total number of nodes in the graph, this is effective in the case of
   *                   disconnected graph
   * @param verticesCount the number of vertices in the graph
   * @return closeness centrality for the given vertex ID
   */
  private def calculateCloseness(id: VertexId,
                                 spMap: SPMap,
                                 normalized: Boolean,
                                 verticesCount: Double): ClosenessData = {
    val connectedVerticesCount = spMap.size.toDouble
    val totalCost = spMap.values.sum
    val closenessValue = if (totalCost > 0.0 && verticesCount > 1.0) {
      val closenessCentrality = (connectedVerticesCount - 1.0) / totalCost
      if (normalized) {
        val factor = (connectedVerticesCount - 1.0) / (verticesCount - 1.0)
        closenessCentrality * factor
      }
      else {
        closenessCentrality
      }
    }
    else {
      0.0
    }
    ClosenessData(id, closenessValue)
  }

  /**
   * The vertex attribute to store the shortest-paths in
   */
  type SPMap = Map[VertexId, Double]

  /**
   * Create the initial shortest-path map for each vertex
   */
  private def makeMap(x: (VertexId, Double)*) = Map(x: _*)

  /**
   * Update the shortest-paths
   */
  private def incrementMap(edge: EdgeTriplet[SPMap, Double]): SPMap = {
    val weight = edge.attr
    edge.dstAttr.map { case (v, d) => v -> (d + weight) }
  }

  /**
   * Merge the shortest-path messages
   */
  private def addMaps(spmap1: SPMap, spmap2: SPMap): SPMap =
    (spmap1.keySet ++ spmap2.keySet).map {
      k => k -> math.min(spmap1.getOrElse(k, Double.MaxValue), spmap2.getOrElse(k, Double.MaxValue))
    }.toMap

  /**
   * Calculates the single source shortest path for each vertex in the graph.
   *
   * @param graph the graph to compute the shortest-paths for its vertices
   * @param getEdgeWeight optional user-defined function that enables the inclusion of the edge weights in the
   *                      shortest-paths calculations by converting the edge attribute type to Double.
   * @tparam VD vertex attribute that is used here to store the shortest-paths attributes
   * @tparam ED the edge attribute that is used here as the edge weight
   * @return the shortest-paths graph
   */
  def calculateShortestPaths[VD, ED: ClassTag](graph: Graph[VD, ED],
                                               getEdgeWeight: Option[ED => Double] = None): Graph[SPMap, Double] = {
    //Initial shortest-path graph
    val shortestPathGraph = graph.mapVertices((id, _) => {
      makeMap(id -> 0)
    }).mapEdges(e => getEdgeWeight match {
      case Some(func) => func(e.attr)
      case _ => 1.0
    })
    //Initial message
    val initialMessage = makeMap()
    //Vertex program
    def vertexProgram(id: VertexId, attr: SPMap, msg: SPMap): SPMap = {
      addMaps(attr, msg)
    }
    //Send message
    def sendMessage(edge: EdgeTriplet[SPMap, Double]): Iterator[(VertexId, SPMap)] = {
      val newAttr = incrementMap(edge)
      if (edge.srcAttr != addMaps(newAttr, edge.srcAttr)) Iterator((edge.srcId, newAttr))
      else Iterator.empty
    }

    Pregel(shortestPathGraph, initialMessage)(vertexProgram, sendMessage, addMaps)
  }
}

/**
 * The closeness centrality calculations
 *
 * @param vertexId the vertex ID
 * @param closenessCentrality the closeness centrality value
 */
case class ClosenessData(vertexId: VertexId, closenessCentrality: Double)

