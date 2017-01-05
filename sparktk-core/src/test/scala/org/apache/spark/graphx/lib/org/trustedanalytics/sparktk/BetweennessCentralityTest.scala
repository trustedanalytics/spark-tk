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
import org.scalautils._
import Tolerance._

class BetweennessCentralityTest extends TestingSparkContextWordSpec with Matchers {

  implicit val tupleEq = new Equality[(Long, Double)] {
    def areEqual(a: (Long, Double), b: Any): Boolean =
      (a, b) match {
        case ((a1: Long, a2: Double), (b1: Long, b2: Float)) => a1 == b1 && a2 === b2.toDouble +- 0.01
        case ((a1: Long, a2: Double), (b1: Long, b2: Double)) => a1 == b1 && a2 === b2.toDouble +- 0.01
        case ((a1: Long, a2: Double), (b1: Int, b2: Double)) => a1 == b1 && a2 === b2.toDouble +- 0.01
        case ((a1: Long, a2: Double), (b1: Int, b2: Float)) => a1 == b1 && a2 === b2.toDouble +- 0.01
        case _ => false
      }
  }

  "Betweenness Centrality" should {
    def getGraph: Graph[String, Double] = {
      // Creates an unlabeled petersen graph
      val vertices = Array(
        (0L, "a"),
        (1L, "b"),
        (2L, "c"),
        (3L, "d"),
        (4L, "e"),
        (5L, "f"),
        (6L, "g"),
        (7L, "h"),
        (8L, "i"),
        (9L, "j"))
      val vRDD = sparkContext.parallelize(vertices)
      // create routes RDD with srcid, destid, distance
      val edges = Array(
        Edge(1L, 3L, 1.0),
        Edge(1L, 4L, 1.0),
        Edge(2L, 5L, 1.0),
        Edge(3L, 5L, 1.0),
        Edge(4L, 2L, 1.0),

        Edge(0L, 2L, 1.0),
        Edge(1L, 6L, 1.0),
        Edge(5L, 7L, 1.0),
        Edge(4L, 8L, 1.0),
        Edge(3L, 9L, 1.0),

        Edge(6L, 0L, 1.0),
        Edge(0L, 9L, 1.0),
        Edge(9L, 8L, 1.0),
        Edge(8L, 7L, 1.0),
        Edge(7L, 6L, 1.0)

      )
      val eRDD = sparkContext.parallelize(edges)
      // define the graph
      Graph(vRDD, eRDD)
    }

    "calculate betweenness centrality on the petersen graph" in {
      val betweennessGraph = BetweennessCentrality.run(getGraph)
      betweennessGraph.vertices.collect().toArray.toList should contain theSameElementsAs (
        List((4, 0.083),
          (0, 0.083),
          (1, 0.083),
          (6, 0.083),
          (3, 0.083),
          (7, 0.083),
          (9, 0.083),
          (8, 0.083),
          (5, 0.083),
          (2, 0.083)))
    }

    "calculate betweenness centrality on a 3 vertex line" in {
      val vertices = Array(
        (1L, "d"),
        (2L, "e"),
        (3L, "j"))
      val vRDD = sparkContext.parallelize(vertices)
      // create routes RDD with srcid, destid, distance
      val edges = Array(
        Edge(1L, 2L, 1.0),
        Edge(2L, 3L, 1.0)
      )
      val eRDD = sparkContext.parallelize(edges)
      // define the graph
      val graph = Graph(vRDD, eRDD)
      val betweennessGraph = BetweennessCentrality.run(graph)
      betweennessGraph.vertices.collect.toArray.toList should contain theSameElementsAs (
        List((1, 0.0), (3, 0.0), (2, 1.0)))
    }

    "calculate betweenness centrality on a 6 vertex grid (ladder graph)" in {
      val vertices = Array(
        (0L, "d"),
        (1L, "e"),
        (2L, "f"),
        (3L, "g"),
        (4L, "h"),
        (5L, "i"))

      val vRDD = sparkContext.parallelize(vertices)
      // create routes RDD with srcid, destid, distance
      val edges = Array(
        Edge(0L, 1L, 1.0),
        Edge(0L, 2L, 1.0),
        Edge(1L, 3L, 1.0),
        Edge(2L, 3L, 1.0),
        Edge(4L, 5L, 1.0),
        Edge(2L, 4L, 1.0),
        Edge(3L, 5L, 1.0)
      )
      val eRDD = sparkContext.parallelize(edges)
      // define the graph
      val graph = Graph(vRDD, eRDD)
      val betweennessGraph = BetweennessCentrality.run(graph, normalize = false)
      betweennessGraph.vertices.collect.toArray.toList should contain theSameElementsAs (
        List((4, 0.83),
          (0, 0.83),
          (1, 0.83),
          (3, 3.33),
          (5, 0.83),
          (2, 3.33)))
    }

    "calculate weighted betweenness centrality" in {
      val vertices = Array(
        (0L, "a"),
        (1L, "d"),
        (2L, "e"),
        (3L, "j"),
        (4L, "k"))
      val vRDD = sparkContext.parallelize(vertices)
      // create routes RDD with srcid, destid, distance
      val edges = Array(
        Edge(0L, 1L, 3),
        Edge(0L, 2L, 2),
        Edge(0L, 3L, 6),
        Edge(0L, 4L, 4),
        Edge(1L, 3L, 5),
        Edge(1L, 5L, 5),
        Edge(2L, 4L, 1),
        Edge(3L, 4L, 2),
        Edge(3L, 5L, 1),
        Edge(4L, 5L, 4)
      )
      val eRDD = sparkContext.parallelize(edges)
      // define the graph
      val graph = Graph(vRDD, eRDD)
      val betweennessGraph = BetweennessCentrality.run(graph, Some((x: Int) => x), normalize = false)
      betweennessGraph.vertices.collect.toArray.toList should contain theSameElementsAs (
        List((4, 4.0), (0, 2.0), (1, 0.0), (3, 3.0), (5, 0.0), (2, 4.0)))
    }

  }
}
