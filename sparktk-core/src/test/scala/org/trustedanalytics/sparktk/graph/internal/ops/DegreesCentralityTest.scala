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
package org.trustedanalytics.sparktk.graph.internal.ops

import org.scalatest.{ WordSpec, Matchers }
import org.trustedanalytics.sparktk.frame.{ Frame, DataTypes, Column, FrameSchema }
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.trustedanalytics.sparktk.graph.{ Graph }
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class DegreeCentralityTest extends TestingSparkContextWordSpec with Matchers {

  private def buildGraph(): Graph = {
    val vertices = FrameSchema(List(Column("id", DataTypes.int32)))
    // This graph is a diamond, 1 to 3 and 4, 3 and 4 to 5
    val vertexRows = FrameRdd.toRowRDD(vertices, sparkContext.parallelize(List(Array[Any](1), Array[Any](3), Array[Any](4), Array[Any](5))))
    val edges = FrameSchema(List(Column("src", DataTypes.int32), Column("dst", DataTypes.int32)))
    val edgeRows = FrameRdd.toRowRDD(edges, sparkContext.parallelize(
      List(Array[Any](1, 3),
        Array[Any](1, 4),
        Array[Any](3, 5),
        Array[Any](4, 5))))

    val edgeFrame = new Frame(edgeRows, edges)
    val vertexFrame = new Frame(vertexRows, vertices)
    val graph = new Graph(vertexFrame, edgeFrame)
    graph
  }

  "degree centrality" when {
    "set with indegree" should {
      "give the indegree centrality" in {
        val graph = buildGraph()
        val degree = graph.degreeCentrality("in")
        degree.schema.columns should equal(List(Column("id", DataTypes.int32), Column("degree_centrality", DataTypes.float64)))
        degree.rdd.toArray.toList should contain theSameElementsAs List(new GenericRow(Array[Any](1, 0.0)), new GenericRow(Array[Any](3, 1.0 / 3.0)), new GenericRow(Array[Any](5, 2.0 / 3.0)), new GenericRow(Array[Any](4, 1.0 / 3.0)))

      }
    }

    "set with outdegree" should {
      "give the outdegree centrality" in {
        val graph = buildGraph()
        val degree = graph.degreeCentrality("out")
        degree.schema.columns should equal(List(Column("id", DataTypes.int32), Column("degree_centrality", DataTypes.float64)))
        degree.rdd.toArray.toList should contain theSameElementsAs List(new GenericRow(Array[Any](1, 2.0 / 3.0)), new GenericRow(Array[Any](3, 1.0 / 3.0)), new GenericRow(Array[Any](5, 0.0)), new GenericRow(Array[Any](4, 1.0 / 3.0)))

      }
    }

    "set with undirected" should {
      "sum the properties going in or out and normalize them" in {
        val graph = buildGraph()
        val degree = graph.degreeCentrality("undirected")
        degree.schema.columns should contain theSameElementsInOrderAs List(Column("id", DataTypes.int32), Column("degree_centrality", DataTypes.float64))
        degree.rdd.toArray.toList should contain theSameElementsAs List(new GenericRow(Array[Any](1, 2.0 / 3.0)), new GenericRow(Array[Any](3, 2.0 / 3.0)), new GenericRow(Array[Any](5, 2.0 / 3.0)), new GenericRow(Array[Any](4, 2.0 / 3.0)))

      }
    }

    "called with an invalid string" should {
      "throw an exception" in {
        val graph = buildGraph()
        val thrown = the[IllegalArgumentException] thrownBy graph.degreeCentrality("invalid")
        thrown.getMessage should equal("requirement failed: Invalid degree option, please choose \"in\", \"out\", or \"undirected\"")
      }
    }

  }
}
