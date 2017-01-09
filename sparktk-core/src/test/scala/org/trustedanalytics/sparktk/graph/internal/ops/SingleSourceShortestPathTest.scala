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

import org.apache.spark.sql.{ SQLContext, Row }
import org.scalatest.Matchers
import org.trustedanalytics.sparktk.graph.Graph
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class SingleSourceShortestPathTest extends TestingSparkContextWordSpec with Matchers {

  "Single source shortest path" should {
    //create Graph of friends in a social network.
    def getGraph: Graph = {
      val sqlContext = new SQLContext(sparkContext)
      // Vertex DataFrame
      val v = sqlContext.createDataFrame(List(
        ("a", "Alice", 34),
        ("b", "Bob", 36),
        ("c", "Charlie", 30),
        ("d", "David", 29),
        ("e", "Esther", 32),
        ("f", "Fanny", 36),
        ("g", "Gabby", 60)
      )).toDF("id", "name", "age")
      val e = sqlContext.createDataFrame(List(
        ("a", "b", "friend", 12),
        ("b", "c", "follow", 2),
        ("c", "b", "follow", 5),
        ("f", "c", "follow", 4),
        ("e", "f", "follow", 8),
        ("e", "d", "friend", 9),
        ("d", "a", "friend", 10),
        ("a", "e", "friend", 3)
      )).toDF("src", "dst", "relationship", "distance")
      // create sparktk graph
      new Graph(v, e)
    }
    //create Graph of friends in a social network with integer vertex IDs.
    def getNewGraph: Graph = {
      val sqlContext: SQLContext = new SQLContext(sparkContext)
      val v = sqlContext.createDataFrame(List(
        (1, "Alice", 34),
        (2, "Bob", 36),
        (3, "Charlie", 30),
        (4, "David", 29),
        (5, "Esther", 32),
        (6, "Fanny", 36),
        (7, "Gabby", 60)
      )).toDF("id", "name", "age")
      val e = sqlContext.createDataFrame(List(
        (1, 2, "friend", 12),
        (2, 3, "follow", 2),
        (3, 2, "follow", 5),
        (6, 3, "follow", 4),
        (5, 6, "follow", 8),
        (5, 4, "friend", 9),
        (4, 1, "friend", 10),
        (1, 5, "friend", 3)
      )).toDF("src", "dst", "relationship", "distance")
      // Create a a sparktk graph
      new Graph(v, e)
    }

    "calculate the single source shortest path" in {
      val singleSourceShortestPathFrame = getGraph.singleSourceShortestPath("a")
      singleSourceShortestPathFrame.collect().head shouldBe Row("b", "Bob", 36, 1.0, "[" + Seq("a", "b").mkString(", ") + "]")
    }

    "calculate the single source shortest paths with edge weights" in {
      val singleSourceShortestPathFrame = getGraph.singleSourceShortestPath("a", Some("distance"))
      singleSourceShortestPathFrame.collect().head shouldBe Row("b", "Bob", 36, 12.0, "[" + Seq("a", "b").mkString(", ") + "]")

    }

    "calculate the single source shortest paths with maximum path length constraint" in {
      val singleSourceShortestPathFrame = getGraph.singleSourceShortestPath("a", None, Some(2))
      singleSourceShortestPathFrame.collect() shouldBe Array(Row("b", "Bob", 36, 1.0, "[" + Seq("a", "b").mkString(", ") + "]"),
        Row("d", "David", 29, 2.0, "[" + Seq("a", "e", "d").mkString(", ") + "]"),
        Row("f", "Fanny", 36, 2.0, "[" + Seq("a", "e", "f").mkString(", ") + "]"),
        Row("a", "Alice", 34, 0.0, "[" + Seq("a").mkString(", ") + "]"),
        Row("c", "Charlie", 30, 2.0, "[" + Seq("a", "b", "c").mkString(", ") + "]"),
        Row("e", "Esther", 32, 1.0, "[" + Seq("a", "e").mkString(", ") + "]"),
        Row("g", "Gabby", 60, Double.PositiveInfinity, "[" + Seq().mkString(", ") + "]"))
    }

    "calculate the single source shortest path for a graph with integer vertex IDs" in {
      val singleSourceShortestPathFrame = getNewGraph.singleSourceShortestPath(1)
      singleSourceShortestPathFrame.collect().head shouldBe Row(4, "David", 29, 2.0, "[" + Seq(1, 5, 4).mkString(", ") + "]")
    }
  }
}
