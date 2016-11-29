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
package org.apache.spark.graphx.lib

import org.apache.spark.graphx._

import scala.reflect.ClassTag

/**
 * Computes Single Source Shortest Paths(SSSP) to the given graph starting for the given vertex ID, returning a graph where each
 * vertex attribute is a map containing the shortest-path distance to each reachable vertex.
 * some optional parameters are provided to constraint the computations for large graph sizes,
 */
object SingleSourceShortestPath {
  /**
    * The SSSP attribute type to be stored at the shortest path graph vertices
    */
  case class ShortestPathType(cost: Double, path: List[VertexId])

  /**
    * Updates the SSSP attribute in each iteration before sending messages to the next super step active vertices,
    * it calculates the new path length or cost in case the edge weight attribute is taken into considerations.
    * @param edge the graph edge
    * @param useEdgeWeight if set to "true" it includes the edge weight in the SSSP calculations.
    * @return SSSP attribute which includes the destination vertex ID, the SSS path vertices and the corresponding SSS path length/cost
    */
  private def updateShortestPath(edge: EdgeTriplet[ShortestPathType, Double], useEdgeWeight: Boolean = false): (ShortestPathType, Double) = {
    val newPath = edge.srcAttr.path :+ edge.dstId
    val weight = if (useEdgeWeight) edge.attr else 1.0
    val newCost = edge.srcAttr.cost + weight
    (new ShortestPathType(newCost, newPath), weight)
  }

  /**
    * Computes SSSP using GraphX Pregel API
    * @param graph the graph to compute SSSP against
    * @param srcVertexId the source vertex ID
    * @param edgeWeightAttribute enable or disable the inclusion of the edge weights in the SSSP calculations
    * @param target destination vertices to limit the SSSP computations
    * @param maxPathLength the maximum path length parameter to limit the SSSP computations
    * @tparam VD vertex attribute that is used here to store the SSSP attributes
    * @tparam ED the edge
    * @return SSSP graph
    */
  def run[VD, ED: ClassTag](graph: Graph[VD, ED],
                            srcVertexId: VertexId,
                            edgeWeightAttribute: Boolean = false,
                            target: Option[Seq[VertexId]] = None,
                            maxPathLength: Option[Double] = None): Graph[ShortestPathType, Double] = {

    /**
      * prepares the message to be used in the next iteration (super step)
      * @param edge  the edge
      * @return the destination vertex ID, SSSP and the corresponding path length/cost
      */
    def sendMessage(edge: EdgeTriplet[ShortestPathType, Double]): Iterator[(VertexId, ShortestPathType)] = {
      val (newShortestPath, weight) = updateShortestPath(edge, edgeWeightAttribute)
      if ((maxPathLength.isDefined && edge.srcAttr.cost == maxPathLength.get) || (target.isDefined && target.get.contains(edge.srcId)) ||
        (edge.srcAttr.cost > edge.dstAttr.cost - weight)) {
        Iterator.empty
      }
      else {
        Iterator((edge.dstId, newShortestPath))
      }
    }

    //Initial graph
    val SpGraph = graph.mapVertices((id, _) => {
      if (id == srcVertexId) ShortestPathType(0.0, List[VertexId](srcVertexId)) else ShortestPathType(Double.PositiveInfinity, List[VertexId]())
    }).mapEdges(e => e.attr.asInstanceOf[Double])

    val initialMessage = ShortestPathType(Double.PositiveInfinity, List[VertexId]())

    Pregel(SpGraph, initialMessage, Int.MaxValue, EdgeDirection.Out)(

      // vertex program
      (id, oldShortestPath, newShortestPath) => if (oldShortestPath.cost < newShortestPath.cost) oldShortestPath else newShortestPath,

      // send message
      sendMessage,

      // merge message
      (a: ShortestPathType, b: ShortestPathType) => if ( a.cost < b.cost) a else b)
  }
}
