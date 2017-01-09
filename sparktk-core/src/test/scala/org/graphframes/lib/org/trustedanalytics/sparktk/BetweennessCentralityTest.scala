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

import org.graphframes._
import org.apache.spark.sql.{ SQLContext, Row }
import org.scalatest.Matchers
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec
import org.scalautils._
import Tolerance._

class BetweennessCentralityTest extends TestingSparkContextWordSpec with Matchers {

  implicit val rowEq = new Equality[Row] {
    def areEqual(a: Row, b: Any): Boolean = {
      b match {
        case b1: Row => b1(0) == a(0) && b1(1) == a(1) && b1(2).asInstanceOf[Double] === a(2).asInstanceOf[Double] +- 0.01
        case _ => false
      }
    }
  }

  "Betweenness Centrality" should {
    def getGraph: GraphFrame = {
      val sqlContext = new SQLContext(sparkContext)
      // Creates an unlabeled petersen graph
      val vertices = sqlContext.createDataFrame(Array(
        (0L, "a"),
        (1L, "b"),
        (2L, "c"),
        (3L, "d"),
        (4L, "e"),
        (5L, "f"),
        (6L, "g"),
        (7L, "h"),
        (8L, "i"),
        (9L, "j"))).toDF("id", "value")
      // create routes RDD with srcid, destid, distance
      val edges = sqlContext.createDataFrame(Array(
        (1L, 3L, 1.0),
        (1L, 4L, 1.0),
        (2L, 5L, 1.0),
        (3L, 5L, 1.0),
        (4L, 2L, 1.0),

        (0L, 2L, 1.0),
        (1L, 6L, 1.0),
        (5L, 7L, 1.0),
        (4L, 8L, 1.0),
        (3L, 9L, 1.0),

        (6L, 0L, 1.0),
        (0L, 9L, 1.0),
        (9L, 8L, 1.0),
        (8L, 7L, 1.0),
        (7L, 6L, 1.0))).toDF("src", "dst", "weight")
      // define the graph
      GraphFrame(vertices, edges)
    }

    "calculate betweenness centrality on the petersen graph" in {
      val betweennessGraph = BetweennessCentrality.run(getGraph)
      betweennessGraph.collect().toArray.toList should contain theSameElementsAs (
        List(new GenericRow(Array(4, "e", 0.083)),
          new GenericRow(Array(0, "a", 0.083)),
          new GenericRow(Array(1, "b", 0.083)),
          new GenericRow(Array(6, "g", 0.083)),
          new GenericRow(Array(3, "d", 0.083)),
          new GenericRow(Array(7, "h", 0.083)),
          new GenericRow(Array(9, "j", 0.083)),
          new GenericRow(Array(8, "i", 0.083)),
          new GenericRow(Array(5, "f", 0.083)),
          new GenericRow(Array(2, "c", 0.083))))

    }

    "calculate betweenness centrality on a 3 vertex line" in {
      val sqlContext = new SQLContext(sparkContext)
      val vertices = sqlContext.createDataFrame(Array(
        (1L, "d"),
        (2L, "e"),
        (3L, "j"))).toDF("id", "value")
      // create routes RDD with srcid, destid, distance
      val edges = sqlContext.createDataFrame(Array(
        (1L, 2L, 1.0),
        (2L, 3L, 1.0)
      )).toDF("src", "dst", "weight")
      // define the graph
      val graph = GraphFrame(vertices, edges)
      val betweennessGraph = BetweennessCentrality.run(graph)
      betweennessGraph.collect.toArray.toList should contain theSameElementsAs (
        List(new GenericRow(Array(1, "d", 0.0)),
          new GenericRow(Array(3, "j", 0.0)),
          new GenericRow(Array(2, "e", 1.0))))
    }

    "calculate betweenness centrality on a 6 vertex grid (ladder graph) graphframes" in {
      val sqlContext = new SQLContext(sparkContext)
      val vertices = sqlContext.createDataFrame(Array(
        (0L, "d"),
        (1L, "e"),
        (2L, "f"),
        (3L, "g"),
        (4L, "h"),
        (5L, "i"))).toDF("id", "value")

      // create routes RDD with srcid, destid, distance
      val edges = sqlContext.createDataFrame(Array(
        (0L, 1L, 1.0),
        (0L, 2L, 1.0),
        (1L, 3L, 1.0),
        (2L, 3L, 1.0),
        (4L, 5L, 1.0),
        (2L, 4L, 1.0),
        (3L, 5L, 1.0)
      )).toDF("src", "dst", "weight")
      // define the graph
      val graph = GraphFrame(vertices, edges)
      val betweennessGraph = BetweennessCentrality.run(graph, normalize = false)
      betweennessGraph.collect.toArray.toList should contain theSameElementsAs (
        List(new GenericRow(Array(4, "h", 0.83)),
          new GenericRow(Array(0, "d", 0.83)),
          new GenericRow(Array(1, "e", 0.83)),
          new GenericRow(Array(5, "i", 0.83)),
          new GenericRow(Array(3, "g", 3.33)),
          new GenericRow(Array(2, "f", 3.33))))
    }

    "calculate weighted betweenness centrality graphframes" in {
      val sqlContext = new SQLContext(sparkContext)
      val vertices = sqlContext.createDataFrame(Array(
        (0L, "a"),
        (1L, "d"),
        (2L, "e"),
        (3L, "j"),
        (4L, "k"),
        (5L, "l"))).toDF("id", "value")
      // create routes RDD with srcid, destid, distance
      val edges = sqlContext.createDataFrame(Array(
        (0L, 1L, 3),
        (0L, 2L, 2),
        (0L, 3L, 6),
        (0L, 4L, 4),
        (1L, 3L, 5),
        (1L, 5L, 5),
        (2L, 4L, 1),
        (3L, 4L, 2),
        (3L, 5L, 1),
        (4L, 5L, 4)
      )).toDF("src", "dst", "weight")
      // define the graph
      val graph = GraphFrame(vertices, edges)
      val betweennessGraph = BetweennessCentrality.run(graph, Some("weight"), normalize = false)
      betweennessGraph.collect.toArray.toList should contain theSameElementsAs (
        List(new GenericRow(Array(0, "a", 2.0)),
          new GenericRow(Array(1, "d", 0.0)),
          new GenericRow(Array(2, "e", 4.0)),
          new GenericRow(Array(3, "j", 3.0)),
          new GenericRow(Array(4, "k", 4.0)),
          new GenericRow(Array(5, "l", 0.0))))
    }

  }
}
