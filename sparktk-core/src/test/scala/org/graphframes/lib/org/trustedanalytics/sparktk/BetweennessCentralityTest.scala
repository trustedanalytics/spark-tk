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

class BetweennessCentralityTest extends TestingSparkContextWordSpec with Matchers {

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
        List(new GenericRow(Array(4, "e", 0.08333333333333333)),
          new GenericRow(Array(0, "a", 0.08333333333333333)),
          new GenericRow(Array(1, "b", 0.08333333333333333)),
          new GenericRow(Array(6, "g", 0.08333333333333333)),
          new GenericRow(Array(3, "d", 0.08333333333333333)),
          new GenericRow(Array(7, "h", 0.08333333333333333)),
          new GenericRow(Array(9, "j", 0.08333333333333333)),
          new GenericRow(Array(8, "i", 0.08333333333333333)),
          new GenericRow(Array(5, "f", 0.08333333333333333)),
          new GenericRow(Array(2, "c", 0.08333333333333333))))

    }

  }
}
