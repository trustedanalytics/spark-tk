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
import org.apache.spark.rdd.RDD

/**
 * Computes Betweenness Centrality of each vertex on the given graph, returning a graph where each
 * vertex has an attribute with the betweenness centrality of that vertex.
 *
 * This calculates the exact betweenness centrality as presented in
 */
object BetweennessCentrality {

  /**
   * Computes Betweenness Centrality using GraphX Pregel API
   *
   * @param graph the graph to compute betweenness centrality on
   * @param getEdgeWeight enable or disable the inclusion of the edge weights in the graph centrality (by default all edge weights are 1)
   * @tparam VD vertex attribute, ignored
   * @tparam ED the edge attribute, potentially used for edge weight
   * @return An RDD with a typle of vertex identity mapped to it's centrality
   */
  def run[VD, ED](graph: Graph[VD, ED],
                  getEdgeWeight: Option[ED => Double] = None): Graph[Double, Double] = {

    // Initialize the edge weights
    val edgeWeightedGraph = graph.mapEdges(
      e => getEdgeWeight match {
        case Some(func) => func(e.attr)
        case None => 1.0
      })

    // Get the graph vertices to iterate over
    val graphVertices = graph.vertices.map({ case (id, _) => (id) }).collect()

    // We normalize against the pairwise number of possible paths in the graph
    // This is ((n-1)*(n02))/2. However the return value of the ventrality
    // algorithm is 2* the centrality value, so I drop the divide by 2
    val normalize = (graphVertices.length - 1) * (graphVertices.length - 2)

    // The current Id is used to initialize the fold
    // remainder is folded over
    val currentId = graphVertices.last
    val remainder = graphVertices.init

    // Calculate the centrality of each vertex, each iteration calculates a partial
    // sum of a single vertex. THese partial sums are then summed and the result
    // Is the un-normalized ventrality value
    val centralityVertexMap: VertexRDD[Double] = remainder.foldLeft(
      calculateVertexCentrality(edgeWeightedGraph, currentId))(
        (accumulator, vertexIndex) => {
          val vertexPartialCentrality = calculateVertexCentrality(edgeWeightedGraph, vertexIndex)

          accumulator.innerJoin(vertexPartialCentrality)(
            (id, valLeft, valRight) => valLeft + valRight)
        })

    // normalize the centrality value
    Graph(centralityVertexMap.map({ case (id: VertexId, x: Double) => (id, x / normalize) }), edgeWeightedGraph.edges)
  }

  // Calculates the partial centrality of a graph from a single vertex. The sum over all vertices is the
  // Vertex centrality
  private def calculateVertexCentrality[VD](initialGraph: Graph[VD, Double], start: VertexId): VertexRDD[Double] = {

    // Initial graph
    // The source vertex has a distance of 0 from the initial vertex, all others
    // are infinite distance from the vertex
    val initializedGraph = initialGraph.mapVertices((id, _) => {
      if (start == id)
        VertexCentralityData(0, 1, false, 0)
      else
        VertexCentralityData(Double.PositiveInfinity, 0, false, 0)
    })

    // Calculate the shortest path using Dijkstra's algorithm
    val shortestPathGraph = calculateShortestPaths(initializedGraph)

    // Initialize the graph to 0's for the partial centrality sum, find the initial horizion
    // The horizon is the furthest most vertices that have not be calculated from the initial vertex
    val shortestPathGraphHorizon = shortestPathInitialHorizon(shortestPathGraph)

    // Sum the vertices from furthest vertex to closest vertex to the initial start vertex
    val centralitiedGraph = partialCentralitySum(shortestPathGraphHorizon, start)

    centralitiedGraph.mapVertices({ case (id, x) => x.sigmaVal }).vertices
  }

  // Calculates the single shortest paths from the given graph vertex
  // Annotates the vertices with the number of shortest paths that go through each vertex
  private def calculateShortestPaths(initializedGraph: Graph[VertexCentralityData, Double]): Graph[VertexCentralityData, Double] = {
    val initialMessage = VertexCentralityData(Double.PositiveInfinity, 1, false, 0)
    // Calculate the shortest path using Dijkstra's algorithm
    val shortestPathGraph = Pregel(initializedGraph, initialMessage)(
      // vertex program
      // This selects the shortest path and updates the number of shortest paths appropriately
      (id, oldShortestPath, newShortestPath) => {
        if (oldShortestPath.distance > newShortestPath.distance)
          newShortestPath
        else if (oldShortestPath.distance < newShortestPath.distance)
          oldShortestPath
        else
          VertexCentralityData(newShortestPath.distance, oldShortestPath.pathCount + newShortestPath.pathCount, false, 0)
      },
      // Increase the search diameter by 1
      (edge) => {
        val newDstPathCount = VertexCentralityData(edge.srcAttr.distance + edge.attr, edge.srcAttr.pathCount, false, 0.0)
        val newSrcPathCount = VertexCentralityData(edge.dstAttr.distance + edge.attr, edge.dstAttr.pathCount, false, 0.0)
        val toMsg = edge.srcAttr.distance < edge.dstAttr.distance - edge.attr
        val fromMsg = edge.dstAttr.distance < edge.srcAttr.distance - edge.attr
        if (toMsg && fromMsg) {
          Iterator((edge.dstId, newDstPathCount), (edge.srcId, newSrcPathCount))
        }
        else if (toMsg && !fromMsg) {
          Iterator((edge.dstId, newDstPathCount))
        }
        else if (!toMsg && fromMsg) {
          Iterator((edge.srcId, newSrcPathCount))
        }
        else {
          Iterator.empty
        }
      },
      // merge message
      // Select the shortest path. If there is a tie, combine the number of ways to this vertex
      (a: VertexCentralityData, b: VertexCentralityData) => {
        if (a.distance < b.distance)
          a
        else if (a.distance > b.distance)
          b
        else
          VertexCentralityData(a.distance, a.pathCount + b.pathCount, false, 0)
      })

    shortestPathGraph
  }

  // Find the initial set of vertices from the start vertex. Mark them as horizon vertices.
  private def shortestPathInitialHorizon(shortestPathGraph: Graph[VertexCentralityData, Double]): Graph[VertexCentralityData, Double] = {
    val shortestPathGraphHorizon: Graph[VertexCentralityData, Double] = shortestPathGraph.pregel(VertexCentralityData(0, 0, false, 0), 2)(
      // If any neighbors have greater distance, this is not a horizon vertex
      (id, currentVertexValue, messageVertexValue) => {
        if (currentVertexValue.distance >= messageVertexValue.distance)
          VertexCentralityData(currentVertexValue.distance, currentVertexValue.pathCount, true, 0)
        else
          VertexCentralityData(currentVertexValue.distance, currentVertexValue.pathCount, false, 0)
      },
      // Send the src distance from source to each to neighbor
      edge => Iterator((edge.dstId, edge.srcAttr), (edge.srcId, edge.dstAttr)),
      // Select the greatest of all distances
      (a, b) => {
        if (a.distance > b.distance)
          a
        else
          b
      })
    shortestPathGraphHorizon
  }

  // sums the recursive values that turn into the partial centrality sum for this
  // particular vertex
  private def partialCentralitySum(shortestPathGraph: Graph[VertexCentralityData, Double], initialId: VertexId): Graph[VertexCentralityData, Double] = {
    val centralitiedGraph = shortestPathGraph.pregel(VertexCentralityData(0, 0, false, 0))(
      // Don't update anything on first iteration
      // Don't update if you are set (not interior)
      // Update if interior, but ALL incoming messages with > distance are marked as not interior (i.e. horizon is on you)
      (id, currentVertexValue, messageVertexValue) => {
        //If the message recieved is a horizon message, the horizon is on the
        // current vertex, which needs to be updated
        if (messageVertexValue.horizon) {
          VertexCentralityData(currentVertexValue.distance, currentVertexValue.pathCount, true, messageVertexValue.sigmaVal)
        }
        // Horizon is not here, don't update
        else {
          currentVertexValue
        }
      },
      // Edge message is simple equation for recursive update
      edge => {
        // If the edges are horizon to horizon, send no message (horizon verices) 
        // are resolved, they don't need information)
        if (edge.srcAttr.horizon && edge.dstAttr.horizon) {
          Iterator.empty
        }
        // if neither the src vertex is in the previous set of the dst vertex
        // nor the dst vertex is in the previous set of the src (i.e. neither is in
        // a shortest path for the other)
        // Send no message
        // This has to be tested both directions since edges are bidirectional
        else if (!(edge.srcAttr.distance - edge.attr == edge.dstAttr.distance) &&
          !(edge.dstAttr.distance - edge.attr == edge.srcAttr.distance)) {
          Iterator.empty
        }
        else {
          // figure out the src is in the previous set of the dst vertex or vice versa
          // if src distance is larger by exactly edges weight, it is in the previous of dst
          // Otherwise (by fall through), the switch (dst is a previous vertex of src)
          val (vertexMessageId, previousVertex, currentVertex) = if (edge.srcAttr.distance - edge.attr == edge.dstAttr.distance) {
            (edge.dstId, edge.dstAttr, edge.srcAttr)
          }
          else {
            (edge.srcId, edge.srcAttr, edge.dstAttr)
          }
          val sigmaUpdate = (previousVertex.pathCount / currentVertex.pathCount) * (1 + currentVertex.sigmaVal)
          // Never update the source vertex, you are calculating the membership of shortest paths between Other
          // Vertices
          if (vertexMessageId == initialId)
            Iterator.empty
          else
            Iterator((vertexMessageId, VertexCentralityData(0, 0, currentVertex.horizon, sigmaUpdate)))
        }
      },
      // sum the partial sums of the shortest path count
      // determine if this is now a horizon vertex (occurs if and only if ALL vertices
      // for which a vertex is in the previous set are horizon
      (a, b) => { VertexCentralityData(0, 0, a.horizon && b.horizon, a.sigmaVal + b.sigmaVal) }
    )
    centralitiedGraph
  }

  // Store all the information relevant to calculating the partial centrality of a particular vertex
  // This information isn't all used at every stage, 0's and falses are used as fillers
  // first distance and pathcount is used,
  // Then the horizon vertices are marked
  // Then the sigmaVal (partial sum of centrality) is calculated
  private case class VertexCentralityData(distance: Double, pathCount: Int, horizon: Boolean, sigmaVal: Double)
}

