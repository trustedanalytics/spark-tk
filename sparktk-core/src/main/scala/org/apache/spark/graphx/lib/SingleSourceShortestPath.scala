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
   * Updates the SSSP attribute in each iteration before sending messages to the next super step active vertices,
   * it calculates the new path length or cost in case the edge weight attribute is taken into considerations.
   *
   * @param edge the graph edge
   * @return SSSP attribute which includes the destination vertex ID, the SSS path vertices and the corresponding SSS path length/cost
   */
  private def updateShortestPath(edge: EdgeTriplet[PathCalculation, Double]): (PathCalculation, Double) = {
    val newPath = edge.srcAttr.path :+ edge.dstId
    val newCost = edge.srcAttr.cost + edge.attr
    (new PathCalculation(newCost, newPath), edge.attr)
  }

  /**
   * Computes SSSP using GraphX Pregel API
   *
   * @param graph the graph to compute SSSP against
   * @param srcVertexId the source vertex ID
   * @param getEdgeWeight enable or disable the inclusion of the edge weights in the SSSP calculations
   * @param targets destination vertices to limit the SSSP computations
   * @param maxPathLength the maximum path length parameter to limit the SSSP computations
   * @tparam VD vertex attribute that is used here to store the SSSP attributes
   * @tparam ED the edge
   * @return SSSP graph
   */
  def run[VD, ED: ClassTag](graph: Graph[VD, ED],
                            srcVertexId: Long,
                            getEdgeWeight: Option[ED => Double] = None,
                            targets: Option[Seq[VertexId]] = None,
                            maxPathLength: Option[Double] = None): Graph[PathCalculation, Double] = {

    require(srcVertexId.toString != null, "Source vertex ID is required to calculate the single source shortest path")
    if (targets.isDefined) {
      require(!targets.get.contains(srcVertexId), s"The vertex ID $srcVertexId cannot be a source vertex and a target vertex at the same time")
    }
    /**
     * prepares the message to be used in the next iteration (super step)
     *
     * @param edge  the edge
     * @return the destination vertex ID, SSSP and the corresponding path length/cost
     */
    def sendMessage(edge: EdgeTriplet[PathCalculation, Double]): Iterator[(VertexId, PathCalculation)] = {
      val (newShortestPath, weight) = updateShortestPath(edge)
      if ((maxPathLength.isDefined && edge.srcAttr.cost >= maxPathLength.get) ||
        (edge.srcAttr.cost > edge.dstAttr.cost - weight)) {
        Iterator.empty
      }
      else {
        Iterator((edge.dstId, newShortestPath))
      }
    }

    //Initial graph
    val ShortestPathGraph = graph.mapVertices((id, _) => {
      if (id == srcVertexId) {
        PathCalculation(0.0, List[VertexId](srcVertexId))
      }
      else {
        PathCalculation(Double.PositiveInfinity, List[VertexId]())
      }
    }).mapEdges(e => getEdgeWeight match {
      case Some(func) => func(e.attr)
      case _ => 1.0
    })

    val initialMessage = PathCalculation(Double.PositiveInfinity, List[VertexId]())

    val ssspGraph = Pregel(ShortestPathGraph, initialMessage, Int.MaxValue, EdgeDirection.Out)(
      // vertex program
      (id, oldShortestPath, newShortestPath) => {
        if (oldShortestPath.cost < newShortestPath.cost)
          oldShortestPath
        else
          newShortestPath
      },
      // send message
      sendMessage,
      // merge message
      (a: PathCalculation, b: PathCalculation) => if (a.cost < b.cost) a else b)
    // filter out the single source shortest path graph based on the given target nodes
    val finalGraph: Graph[PathCalculation, Double] = if (targets.isDefined) {
      ssspGraph.subgraph(vpred = (id, shoretestPath) => targets.get.contains(id) || srcVertexId == id)
    }
    else {
      ssspGraph
    }
    finalGraph
  }
}

/**
 * The single source shortest path attribute to be stored at the shortest path graph vertices
 *
 * @param cost the shortest path cost/distance
 * @param path the shortest path
 */
case class PathCalculation(cost: Double, path: List[VertexId])
