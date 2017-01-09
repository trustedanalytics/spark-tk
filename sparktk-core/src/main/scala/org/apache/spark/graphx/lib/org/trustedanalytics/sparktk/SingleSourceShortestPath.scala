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
 * Computes the Single Source Shortest Paths (SSSP)for the given graph starting from the given vertex ID,
 * it returns a graph where each vertex attribute contains the shortest path to this vertex and the corresponding cost.
 * It utilizes a distributed version of Dijkstra-based shortest path algorithm. Some optional parameters, e.g., maximum path length, are provided
 * to constraint the computations for large graphs.
 */
object SingleSourceShortestPath {

  /**
   * Updates the SSSP attribute in each iteration before sending messages to the next super-step active vertices,
   * it calculates the shortest path and the new path length or cost in case the edge weight is considered.
   *
   * @param edge the graph edge
   * @return SSSP attribute which contains the shortest path and the corresponding cost
   */
  private def updateShortestPath[T](edge: EdgeTriplet[PathCalculation[T], Double]): (PathCalculation[T], Double) = {

    val newPath = edge.srcAttr.path :+ edge.dstAttr.origVertexId
    val newCost = edge.srcAttr.cost + edge.attr
    (new PathCalculation(newCost, newPath, edge.dstAttr.origVertexId), edge.attr)
  }

  /**
   * Computes SSSP using GraphX Pregel API
   *
   * @param graph the graph to compute SSSP against
   * @param srcVertexId the source vertex ID
   * @param getEdgeWeight optional user-defined function that enables the inclusion of the edge weights in the SSSP
   *                      calculations by converting the edge attribute type to Double.
   * @param maxPathLength optional maximum path length or cost to limit the SSSP computations
   * @param getOrigVertexId user-defined function that converts the vertex attribute to a given type
   * @tparam VD vertex attribute that is used here to store the SSSP attributes
   * @tparam ED the edge attribute that is used here as the edge weight
   * @return the SSSP graph
   */
  def run[VD, ED: ClassTag, T](graph: Graph[VD, ED],
                               srcVertexId: Long,
                               getEdgeWeight: Option[ED => Double] = None,
                               maxPathLength: Option[Double] = None,
                               getOrigVertexId: VD => T): Graph[PathCalculation[T], Double] = {
    require(srcVertexId.toString != null, "Source vertex ID is required to calculate the single source shortest path")
    /**
     * Prepares the message to be used in the next iteration
     *
     * @param edge  the edge
     * @return the destination vertex ID, SSSP and the corresponding path cost
     */
    def sendMessage[T](edge: EdgeTriplet[PathCalculation[T], Double]): Iterator[(VertexId, PathCalculation[T])] = {
      val (newShortestPath, weight) = updateShortestPath(edge)
      if ((maxPathLength.isDefined && edge.srcAttr.cost + weight > maxPathLength.get) ||
        (edge.srcAttr.cost + weight >= edge.dstAttr.cost)) {
        Iterator.empty
      }
      else if (edge.srcAttr.path.toSet.contains(edge.dstAttr.origVertexId)) { // Avoid cycles
        Iterator.empty
      }
      else {
        Iterator((edge.dstId, newShortestPath))
      }
    }
    //Initial graph
    val ShortestPathGraph = graph.mapVertices((id, attr) => {
      val origVertexId = getOrigVertexId(attr)
      if (id == srcVertexId) {
        PathCalculation(0.0, List[T](origVertexId), origVertexId)
      }
      else {
        PathCalculation(Double.PositiveInfinity, List[T](), origVertexId)
      }
    }).mapEdges(e => getEdgeWeight match {
      case Some(func) => func(e.attr)
      case _ => 1.0
    })
    //Initial message
    val initialMessage = PathCalculation[T](Double.PositiveInfinity, List[T](), null.asInstanceOf[T])
    Pregel(ShortestPathGraph, initialMessage, Int.MaxValue, EdgeDirection.Out)(
      // vertex program
      (id, oldShortestPath, newShortestPath) => {
        if (oldShortestPath.cost <= newShortestPath.cost)
          oldShortestPath
        else
          newShortestPath
      },
      // send message
      sendMessage[T],
      // merge message
      (a: PathCalculation[T], b: PathCalculation[T]) => if (a.cost < b.cost) {
        a
      }
      else {
        b
      })
  }
}

/**
 * The single source shortest path attribute to be stored at the shortest path graph vertices
 *
 * @param cost the shortest path cost/length
 * @param path the shortest path
 * @param origVertexId the original vertex ID
 */
case class PathCalculation[T](cost: Double, path: List[T], origVertexId: T)
