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
import org.apache.spark.sql.{ Row, DataFrame }
import org.apache.spark.sql.functions.{ udf, col }
import org.graphframes.GraphFrame
import org.graphframes.lib.GraphXConversions

/**
 * Computes the Single Source Shortest Path (SSSP) for the graph starting from the given vertex ID to every vertex in the graph.
 * The algorithm returns the shortest path from the source vertex and the corresponding cost.
 * This implementation utilizes a distributed version of Dijkstra's shortest path algorithm.
 * Some optional parameters, e.g., maximum path length, constrain the computations for large graphs.
 */
object SingleSourceShortestPath {

  /**
   * Computes the single source shortest path for the graph frame using Graphx-based single source shortest path
   * algorithm
   *
   * @param graph graph to compute SSSP against
   * @param srcVertexId source vertex ID
   * @param edgePropName optional edge column name to be used as edge weight
   * @param maxPathLength optional maximum path length or cost to limit the SSSP computations
   * @return dataframe with the shortest path and corresponding cost from the source vertex to each target vertex ID.
   */
  def run(graph: GraphFrame,
          srcVertexId: Any,
          edgePropName: Option[String] = None,
          maxPathLength: Option[Double] = None): DataFrame = {
    val edgeWeightFunc: Option[(Row) => Double] = getEdgeWeightFunc(graph, edgePropName)
    val origVertexIdFunc = (x: Row) => x.get(x.fieldIndex(GraphFrame.ID)).toString
    val ssspGraphx = sparktk.SingleSourceShortestPath.run(graph.toGraphX,
      GraphXConversions.integralId(graph, srcVertexId),
      edgeWeightFunc,
      maxPathLength,
      origVertexIdFunc)
    val ssspGraphFrame = GraphXConversions.fromGraphX(graph, ssspGraphx, Seq(SSSP_RESULTS))
    getSingleSourceShortestPathFrame(ssspGraphFrame)
  }

  /**
   *
   * @param ssspGraphFrame graph with the single source shortest path calulations
   * @return dataframe with the original vertices columns, in addition to the shortest path and
   *         corresponding cost from the source vertex to each target vertex ID.
   */
  def getSingleSourceShortestPathFrame(ssspGraphFrame: GraphFrame): DataFrame = {
    val costUdf = udf { (row: Row) => row.getDouble(0) }
    val pathUdf = udf { (row: Row) => "[" + row.getSeq[String](1).map(s => s).mkString(", ") + "]" }
    val costDataFrame = ssspGraphFrame.vertices.withColumn(SSSP_COST, costUdf(col(SSSP_RESULTS)))
    costDataFrame.withColumn(SSSP_PATH, pathUdf(col(SSSP_RESULTS))).drop(SSSP_RESULTS)
  }

  /**
   * Get the edge weight function that enables the inclusion of the edge weights in the SSSP
   * calculations by converting the edge attribute type to Double
   *
   * @param graph graph to compute SSSP against
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
  private val SSSP_RESULTS = "results"
  private val SSSP_COST = "cost"
  private val SSSP_PATH = "path"
}

