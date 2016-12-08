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
      val singleSourceShortestPathGraph = SingleSourceShortestPath.run(getGraph, 1)
      singleSourceShortestPathGraph.vertices.collect shouldBe Array((4, PathCalculation(2.0, List(1, 2, 4))),
        (1, PathCalculation(0.0, List(1))),
        (6, PathCalculation(1.0, List(1, 6))),
        (3, PathCalculation(2.0, List(1, 2, 3))),
        (7, PathCalculation(1.0, List(1, 7))),
        (5, PathCalculation(3.0, List(1, 2, 3, 5))),
        (2, PathCalculation(1.0, List(1, 2))))
    }

    "calculate the single source shortest paths with edge weights" in {
      val singleSourceShortestPathGraph = SingleSourceShortestPath.run(getGraph, 1, Some((x: Double) => x))
      singleSourceShortestPathGraph.vertices.collect shouldBe Array((4, PathCalculation(2700.0, List(1, 2, 4))),
        (1, PathCalculation(0.0, List(1))),
        (6, PathCalculation(500.0, List(1, 6))),
        (3, PathCalculation(2600.0, List(1, 2, 3))),
        (7, PathCalculation(1100.0, List(1, 7))),
        (5, PathCalculation(3300.0, List(1, 2, 3, 5))),
        (2, PathCalculation(1800.0, List(1, 2))))
    }

    "calculate the single source shortest paths with maximum path length constraint" in {
      val singleSourceShortestPathGraph = SingleSourceShortestPath.run(getGraph, 1, None, Some(2))
      singleSourceShortestPathGraph.vertices.collect shouldBe Array((4, PathCalculation(2.0, List(1, 2, 4))),
        (1, PathCalculation(0.0, List(1))),
        (6, PathCalculation(1.0, List(1, 6))),
        (3, PathCalculation(2.0, List(1, 2, 3))),
        (7, PathCalculation(1.0, List(1, 7))),
        (5, PathCalculation(Double.PositiveInfinity, List())),
        (2, PathCalculation(1.0, List(1, 2))))
    }
  }
}
