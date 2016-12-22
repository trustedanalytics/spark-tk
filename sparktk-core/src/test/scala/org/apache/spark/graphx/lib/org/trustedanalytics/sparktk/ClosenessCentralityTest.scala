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

import org.apache.spark.graphx.{ Edge, Graph }
import org.scalatest.Matchers
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class ClosenessCentralityTest extends TestingSparkContextWordSpec with Matchers {

  "Single source shortest path" should {
    def getGraph: Graph[String, Double] = {
      // create vertices RDD with ID and Name
      val vertices = Array((1L, "Ben"),
        (2L, "Anna"),
        (3L, "Cara"),
        (4L, "Dana"),
        (5L, "Evan"),
        (6L, "Frank"))
      val vRDD = sparkContext.parallelize(vertices)
      // create routes RDD with srcid, destid, distance
      val edges = Array(Edge(1L, 2L, 1800.0),
        Edge(2L, 3L, 800.0),
        Edge(3L, 4L, 600.0),
        Edge(3L, 5L, 900.0),
        Edge(4L, 5L, 1100.0),
        Edge(4L, 6L, 700.0),
        Edge(5L, 6L, 500.0))
      val eRDD = sparkContext.parallelize(edges)
      // create the graph
      Graph(vRDD, eRDD)
    }

    "calculate the closeness centrality with normalized values" in {
      val closenessCentrality = ClosenessCentrality.run(getGraph)
      assert(closenessCentrality(3) == ClosenessCalculations(3, 0.44999999999999996))
    }

    "calculate the closeness centrality" in {
      val closenessCentrality = ClosenessCentrality.run(getGraph, None, normalized = false)
      assert(closenessCentrality(3) == ClosenessCalculations(3, 0.75))
    }

    "calculate the closeness centrality with edge weights normalized" in {
      val closenessCentrality = ClosenessCentrality.run(getGraph, Some((x: Double) => x))
      assert(closenessCentrality(3) == ClosenessCalculations(3, 6.428571428571428E-4))
    }

    "calculate the closeness centrality with edge weights" in {
      val closenessCentrality = ClosenessCentrality.run(getGraph, Some((x: Double) => x), normalized = false)
      assert(closenessCentrality(3) == ClosenessCalculations(3, 0.0010714285714285715))
    }
  }
}
