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
import org.graphframes.GraphFrame
import org.graphframes.lib.GraphXConversions
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD

/**
 * Computes the betweenness centrality exactly on the given graph.
 */
object BetweennessCentrality {

  private val betweennessResults = "betweenness_results"
  /**
   * Computes the betweeness centrality for the vertices of the graph using Graphx-based betweenness centrality
   * algorithm
   *
   * @param graph the graph to compute betweenness centrality on
   * @param edgePropName optional edge column name to be used as edge weight
   * @return the target vertexID, the shortest path from the source vertex and the corresponding cost
   */
  def run(graph: GraphFrame, edgePropName: Option[String] = None): DataFrame = {
    // Convert to grpahx
    val gf = GraphFrame(graph.vertices.select(GraphFrame.ID), graph.edges)

    // calculate the betweenness centrality
    val graphxBetweennessRDD = edgePropName match {
      case Some(edgeName) =>
        val edgeWeightType = graph.edges.schema(edgeName).dataType
        sparktk.BetweennessCentrality.run(graph.toGraphX, getEdgeWeightFunc(graph, edgePropName))
      case None => sparktk.BetweennessCentrality.run(gf.toGraphX)
    }
    // return an RDD representing the betweenness value on vertices
    GraphXConversions.fromGraphX(graph, graphxBetweennessRDD, Seq(betweennessResults)).vertices
  }

  private def getEdgeWeightFunc(graph: GraphFrame, edgePropName: Option[String]): Option[(Row) => Double] = {
    val edgeWeightFunc = if (edgePropName.isDefined) {
      val edgeWeightType = graph.edges.schema(edgePropName.get).dataType
      require(edgeWeightType.isInstanceOf[NumericType], "The edge weight type should be numeric")
      Some((row: Row) => row.getAs[Any](edgePropName.get) match {
        case x: Int => x.toDouble
        case x: Long => x.toDouble
        case x: Short => x.toDouble
        case x: Byte => x.toDouble
        case _ => throw new scala.ClassCastException(s"the edge weight type cannot be $edgeWeightType")
      })
    }
    else {
      None
    }
    edgeWeightFunc
  }
}