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
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, DataFrame}
import org.graphframes.GraphFrame
import org.graphframes.lib.GraphXConversions
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Computes the Single Source Shortest Paths (SSSP)for the given graph starting from the given vertex ID,
  * it returns the target vertexID, he shortest path from the source vertex and the corresponding cost.
  * It utilizes a distributed version of Dijkstra-based shortest path algorithm. Some optional parameters, e.g., maximum path length, are provided
  * to constraint the computations for large graphs.
 */
object SingleSourceShortestPath {

  /**
   * Computes the single source shortest path for the given graph frame using Graphx-based single source shortest path
   * algorithm
   *
   * @param graph the graph to compute SSSP against
   * @param srcVertexId source vertex ID
   * @param edgePropName optional edge column name to be used as edge weight
   * @param maxPathLength optional maximum path length or cost to limit the SSSP computations
   * @return vertices dataframe with two additional nodes for the shortest path and the cost
   */
  def run(graph: GraphFrame, srcVertexId: Any,
          edgePropName: Option[String] = None,
          maxPathLength: Option[Double] = None): DataFrame = {
    // get the vertex IDs map for the given graph frame
    val vertexIdsMap = convertVertexIds(graph)
    val gf = GraphFrame(graph.vertices.select(GraphFrame.ID),graph.edges)
    // calculate the single source shortest path
    val graphxSsspGraph =
      if (edgePropName.isDefined) {
        val edgeWeightType = graph.edges.schema(edgePropName.get).dataType
        require(edgeWeightType.isInstanceOf[NumericType], "The edge weight type should be numeric")
        val graphxGraph = gf.toGraphX.mapEdges(e => e.attr.getAs[Int](edgePropName.get))
        sparktk.SingleSourceShortestPath.run(graphxGraph,
                                             GraphXConversions.integralId(graph, srcVertexId),
                                             Some((x: Int) => x.toDouble),
                                             maxPathLength)
      }else {
        sparktk.SingleSourceShortestPath.run(gf.toGraphX,
                                             GraphXConversions.integralId(graph, srcVertexId),
                                             None,
                                             maxPathLength)
      }
    // convert the single source shortest path graph to a graph frame
    val g = GraphXConversions.fromGraphX(gf, graphxSsspGraph, vertexNames = Seq(SSSP_RESULTS))
    getSingleSourceShortestPathResults(g.vertices,vertexIdsMap)
  }
  /**
    * Store the given graph frame vertex IDs and its corresponding IDs in Graphx
    *
    * @param graphFrame graph frame
    * @return vertex IDs for the graphx and the graphframes graphs
    */
  def convertVertexIds(graphFrame: GraphFrame):Map[Long, Any] = {
    var vertexIdsMap = mutable.Map[Long,Any]()
    val idBuffer = new ArrayBuffer[Any]()
    val idIterator = graphFrame.vertices.mapPartitions({iter =>
      while(iter.hasNext){
        idBuffer += iter.next.getAs[Any](GraphFrame.ID)
      }
      idBuffer.toIterator
    })
    idIterator.collect.foreach(id => vertexIdsMap += (GraphXConversions.integralId(graphFrame,id) -> id))
    vertexIdsMap.toMap
  }
  /**
    * Get the single source shortest path results, the target vertex ID, and its cost and shortest path
    *
    * @param vertices  the single source shortest path vertices
    * @return single source shortest path results
    */
  def getSingleSourceShortestPathResults(vertices: DataFrame, vertexIdsMap:Map[Long,Any]): DataFrame = {
    // get the vertex type
    val idType = vertices.schema(GraphFrame.ID).dataType
    // split the single source shortest path cost and path into two columns
    val shortestPathRdd = vertices.map{ case Row(id:Any,Row(cost: Double, path: mutable.WrappedArray[Long])) =>
      Row(id, cost, path.map(id => vertexIdsMap(id)).toSet.toString())}
    val schema = new StructType(Array(new StructField(GraphFrame.ID, idType),
      new StructField(SSSP_COST, DoubleType),
      new StructField(SSSP_PATH,StringType)))
    vertices.sqlContext.createDataFrame(shortestPathRdd, schema)
  }
  private val SSSP_RESULTS = "results"
  private val SSSP_COST = "cost"
  private val SSSP_PATH = "path"
}

