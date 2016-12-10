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

    "calculate the single source shortest path" in {
      val singleSourceShortestPathFrame = getGraph.singleSourceShortestPath("a")
      singleSourceShortestPathFrame.collect().head shouldBe Row("b", 1.0, "Set(a, b)")
    }

    "calculate the single source shortest paths with edge weights" in {
      val singleSourceShortestPathFrame = getGraph.singleSourceShortestPath("a", Some("distance"))
      println(singleSourceShortestPathFrame.collect().mkString("\n"))
      singleSourceShortestPathFrame.collect().head shouldBe Row("b", 12.0, "Set(a, b)")

    }

    "calculate the single source shortest paths with maximum path length constraint" in {
      val singleSourceShortestPathFrame = getGraph.singleSourceShortestPath("a", None, Some(2))
      singleSourceShortestPathFrame.collect() shouldBe Array(Row("b", 1.0, "Set(a, b)"),
        Row("d", 2.0, "Set(a, e, d)"),
        Row("f", 2.0, "Set(a, e, f)"),
        Row("a", 0.0, "Set(a)"),
        Row("c", 2.0, "Set(a, b, c)"),
        Row("e", 1.0, "Set(a, e)"),
        Row("g", Double.PositiveInfinity, "Set()"))
    }
  }
}
