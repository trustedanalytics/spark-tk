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

import org.apache.spark.sql.SQLContext
import org.graphframes._
import org.scalatest.Matchers
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class ClosenessCentralityTest extends TestingSparkContextWordSpec with Matchers {

  "Closeness centrality" should {
    def getGraph: GraphFrame = {
      val sqlContext: SQLContext = new SQLContext(sparkContext)
      // Vertex DataFrame
      val v = sqlContext.createDataFrame(List(("a", "Ben"),
        ("b", "Anna"),
        ("c", "Cara"),
        ("d", "Dana"),
        ("e", "Evan"),
        ("f", "Frank"))).toDF("id", "name")
      val e = sqlContext.createDataFrame(List(
        ("a", "b", 3.0),
        ("b", "c", 12.0),
        ("c", "d", 2.0),
        ("c", "e", 5.0),
        ("d", "e", 4.0),
        ("d", "f", 8.0),
        ("e", "f", 9.0))).toDF("src", "dst", "distance")
      // Create a GraphFrame
      GraphFrame(v, e)
    }
    // create disconnected graph
    def getDisconnectedGraph: GraphFrame = {
      val sqlContext: SQLContext = new SQLContext(sparkContext)
      // Vertex DataFrame
      val v = sqlContext.createDataFrame(List(("a", "Ben"),
        ("b", "Anna"),
        ("c", "Cara"),
        ("d", "Dana"),
        ("e", "Evan"),
        ("f", "Frank"),
        ("g", "Farid"),
        ("h", "Hana"))).toDF("id", "name")
      val e = sqlContext.createDataFrame(List(
        ("a", "b", 3.0),
        ("b", "c", 12.0),
        ("c", "d", 2.0),
        ("c", "e", 5.0),
        ("d", "e", 4.0),
        ("d", "f", 8.0),
        ("e", "f", 9.0),
        ("g", "h", 10.0))).toDF("src", "dst", "distance")
      // Create a GraphFrame
      GraphFrame(v, e)
    }

    "calculate the closeness centrality with normalized values" in {
      val closenessCentralityGraph = ClosenessCentrality.run(getGraph)
      closenessCentralityGraph.vertices.collect().head.getAs[Double]("ClosenessCentrality") shouldBe (0.4 +- 1E-6)
    }

    "calculate the closeness centrality" in {
      val closenessCentralityGraph = ClosenessCentrality.run(getGraph, None, normalized = false)
      closenessCentralityGraph.vertices.collect().head.getAs[Double]("ClosenessCentrality") shouldBe (0.5 +- 1E-6)
    }

    "calculate the normalized closeness centrality with edge weights" in {
      val closenessCentralityGraph = ClosenessCentrality.run(getGraph, Some("distance"))
      closenessCentralityGraph.vertices.collect().head.getAs[Double]("ClosenessCentrality") shouldBe (0.04923076923076924 +- 1E-6)
    }

    "calculate the closeness centrality with edge weights" in {
      val closenessCentralityGraph = ClosenessCentrality.run(getGraph, Some("distance"), normalized = false)
      closenessCentralityGraph.vertices.collect().head.getAs[Double]("ClosenessCentrality") shouldBe (0.06153846153846154 +- 1E-6)
    }

    "calculate the closeness centrality for a disconnected graph" in {
      val closenessCentralityGraph = ClosenessCentrality.run(getDisconnectedGraph, None, normalized = false)
      closenessCentralityGraph.vertices.collect().head.getAs[Double]("ClosenessCentrality") shouldBe (0.5 +- 1E-6)
    }

    "calculate the normalized closeness centrality for a disconnected graph" in {
      val closenessCentralityGraph = ClosenessCentrality.run(getDisconnectedGraph, None)
      closenessCentralityGraph.vertices.collect().head.getAs[Double]("ClosenessCentrality") shouldBe (0.2857142857142857 +- 1E-6)
    }
  }
}
