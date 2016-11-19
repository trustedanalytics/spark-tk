/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.graphframes.lib

import java.util

import org.apache.spark.graphx.{lib => graphxlib}
import org.apache.spark.sql.functions.{callUDF, col}
import org.apache.spark.sql.types.{IntegerType, MapType}
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.graphframes.GraphFrame

import scala.collection.JavaConverters._

/**
 * Computes shortest paths from every vertex to the given set of landmark vertices.
 * Note that this takes edge direction into account.
 *
 * The returned DataFrame contains all the original vertex information as well as one additional
 * column:
 *  - distances (`MapType[vertex ID type, IntegerType]`): For each vertex v, a map containing
 *    the shortest-path distance to each reachable landmark vertex.
 */
class SingleSourceShortestPaths private[graphframes](private val graph: GraphFrame) extends Arguments {
  private var lmarks: Option[Seq[Any]] = None

  /**
   * The list of landmark vertex ids. Shortest paths will be computed to each landmark.
   */
  def landmarks(value: Seq[Any]): this.type = {
    // TODO(tjh) do some initial checks here, without running queries.
    lmarks = Some(value)
    this
  }

  /**
   * The list of landmark vertex ids. Shortest paths will be computed to each landmark.
   */
  def landmarks(value: util.ArrayList[Any]): this.type = {
    landmarks(value.asScala)
  }

  def run(): DataFrame = {
    SingleSourceShortestPaths.run(graph, check(lmarks, "landmarks"))
  }
}

object SingleSourceShortestPaths {

  private def run(graph: GraphFrame, landmarks: Seq[Any]): DataFrame = ???/*{
    val idType = graph.vertices.schema(GraphFrame.ID).dataType
    val longIdToLandmark = landmarks.map(l => GraphXConversions.integralId(graph, l) -> l).toMap
    val gx = graphxlib.SingleSourceShortestPath.run(
      graph.cachedTopologyGraphX,
      longIdToLandmark.keys.toSeq.sorted).mapVertices { case (_, m) => m.toSeq }
    val g = GraphXConversions.fromGraphX(graph, gx, vertexNames = Seq(DISTANCE_ID))
    val distanceCol: Column = if (graph.hasIntegralIdType) {
      // It seems there are no easy way to convert a sequence of pairs into a map
      def mapToLandmark(distances: Seq[Row]): Map[Any, Int] = {
        distances.map { case Row(k: Long, v: Int) =>
          k -> v
        }.toMap
      }
      callUDF(mapToLandmark _, MapType(idType, IntegerType, false), g.vertices(DISTANCE_ID))
        .cast(MapType(idType, IntegerType, false))
    } else {
      def mapToLandmark(distances: Seq[Row]): Map[Any, Int] = {
        distances.map { case Row(k: Long, v: Int) =>
          longIdToLandmark(k) -> v
        }.toMap
      }
      callUDF(mapToLandmark _, MapType(idType, IntegerType, false), col(DISTANCE_ID))
    }
    val cols = graph.vertices.columns.map(col) :+ distanceCol.as(DISTANCE_ID)
    g.vertices.select(cols: _*)
  }
*/
  private val DISTANCE_ID = "distances"

}
