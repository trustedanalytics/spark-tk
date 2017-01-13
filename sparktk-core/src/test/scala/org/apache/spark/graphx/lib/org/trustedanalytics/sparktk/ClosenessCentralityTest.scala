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

  "Closeness centrality" should {
    // create a connected graph
    def getConnectedGraph: Graph[String, Double] = {
      // create vertices RDD with ID and Name
      val vertices = Array((1L, "Ben"),
        (2L, "Anna"),
        (3L, "Cara"),
        (4L, "Dana"),
        (5L, "Evan"),
        (6L, "Frank"))
      val vRDD = sparkContext.parallelize(vertices)
      // create routes RDD with srcid, destid, distance
      val edges = Array(Edge(1L, 2L, 3.0),
        Edge(2L, 3L, 12.0),
        Edge(3L, 4L, 2.0),
        Edge(3L, 5L, 5.0),
        Edge(4L, 5L, 4.0),
        Edge(4L, 6L, 8.0),
        Edge(5L, 6L, 9.0))
      val eRDD = sparkContext.parallelize(edges)
      // create the graph
      Graph(vRDD, eRDD)
    }
    // create disconnected graph
    def getDisconnectedGraph: Graph[String, Double] = {
      // create vertices RDD with ID and Name
      val vertices = Array((1L, "Ben"),
        (2L, "Anna"),
        (3L, "Cara"),
        (4L, "Dana"),
        (5L, "Evan"),
        (6L, "Frank"),
        (7L, "Wafa"),
        (8L, "Anna"))
      val vRDD = sparkContext.parallelize(vertices)
      // create routes RDD with srcid, destid, distance
      val edges = Array(Edge(1L, 2L, 3.0),
        Edge(2L, 3L, 12.0),
        Edge(3L, 4L, 2.0),
        Edge(3L, 5L, 5.0),
        Edge(4L, 5L, 4.0),
        Edge(4L, 6L, 8.0),
        Edge(5L, 6L, 9.0),
        Edge(7L, 8L, 10.0))
      val eRDD = sparkContext.parallelize(edges)
      // create the graph
      Graph(vRDD, eRDD)
    }

    "calculate the closeness centrality with normalized values" in {
      val closenessCentralityGraph = ClosenessCentrality.run(getConnectedGraph)
      closenessCentralityGraph.vertices.collect().toMap.get(3).get shouldBe (0.44999999999999996 +- 1E-6)
    }

    "calculate the closeness centrality" in {
      val closenessCentralityGraph = ClosenessCentrality.run(getConnectedGraph, None, normalize = false)
      closenessCentralityGraph.vertices.collect().toMap.get(3).get shouldBe (0.75 +- 1E-6)
    }

    "calculate the normalized closeness centrality with edge weights" in {
      val closenessCentralityGraph = ClosenessCentrality.run(getConnectedGraph, Some((x: Double) => x))
      closenessCentralityGraph.vertices.collect().toMap.get(3).get shouldBe (0.10588235294117647 +- 1E-6)
    }

    "calculate the closeness centrality with edge weights" in {
      val closenessCentralityGraph = ClosenessCentrality.run(getConnectedGraph, Some((x: Double) => x), normalize = false)
      closenessCentralityGraph.vertices.collect().toMap.get(3).get shouldBe (0.17647058823529413 +- 1E-6)
    }

    "calculate the closeness centrality for a disconnected graph" in {
      val closenessCentralityGraph = ClosenessCentrality.run(getDisconnectedGraph, None, normalize = false)
      closenessCentralityGraph.vertices.collect().toMap.get(3).get shouldBe (0.75 +- 1E-6)
    }

    "calculate the normalized closeness centrality for a disconnected graph" in {
      val closenessCentralityGraph = ClosenessCentrality.run(getDisconnectedGraph, None)
      closenessCentralityGraph.vertices.collect().toMap.get(3).get shouldBe (0.3214285714285714 +- 1E-6)
    }
  }
}
