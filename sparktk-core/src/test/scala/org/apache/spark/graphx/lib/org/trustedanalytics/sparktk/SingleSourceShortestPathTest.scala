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
import org.apache.spark.sql.Row
import org.scalatest.Matchers
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class SingleSourceShortestPathTest extends TestingSparkContextWordSpec with Matchers {

  "Single source shortest path" should {
    def getGraph: Graph[String, Double] = {
      // create vertices RDD with ID and Name
      val vertices = Array((1L, "SFO"),
        (2L, "ORD"),
        (3L, "DFW"),
        (4L, "PDX"),
        (5L, "LAX"),
        (6L, "LLL"),
        (7L, "IAF"))
      val vRDD = sparkContext.parallelize(vertices)
      // create routes RDD with srcid, destid, distance
      val edges = Array(Edge(1L, 2L, 1800.0),
        Edge(2L, 3L, 800.0),
        Edge(3L, 1L, 1400.0),
        Edge(2L, 4L, 900.0),
        Edge(1L, 7L, 1100.0),
        Edge(3L, 5L, 700.0),
        Edge(4L, 6L, 600.0),
        Edge(1L, 6L, 500.0))
      val eRDD = sparkContext.parallelize(edges)
      // create the graph
      Graph(vRDD, eRDD)
    }

    "calculate the single source shortest path" in {
      val singleSourceShortestPathGraph = SingleSourceShortestPath.run(getGraph, 1, None, None, (x: String) => x)
      singleSourceShortestPathGraph.vertices.collect.head shouldBe (4, PathCalculation(2.0, List("SFO", "ORD", "PDX"), "PDX"))
    }
    "calculate the single source shortest paths with edge weights" in {
      val singleSourceShortestPathGraph = SingleSourceShortestPath.run(getGraph, 1, Some((x: Double) => x), None, (t: String) => t)
      singleSourceShortestPathGraph.vertices.collect.head shouldBe (4, PathCalculation(2700.0, List("SFO", "ORD", "PDX"), "PDX"))
    }
    "calculate the single source shortest path for a disconnected node" in {
      val singleSourceShortestPathGraph = SingleSourceShortestPath.run(getGraph, 7, None, None, (x: String) => x)
      println(singleSourceShortestPathGraph.vertices.collect.mkString("\n"))
      singleSourceShortestPathGraph.vertices.collect.head shouldBe (4, PathCalculation(Double.PositiveInfinity, List(), "PDX"))
    }
  }
}
