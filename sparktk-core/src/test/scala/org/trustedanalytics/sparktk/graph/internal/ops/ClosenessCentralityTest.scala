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

import org.apache.spark.sql.SQLContext
import org.scalatest.Matchers
import org.trustedanalytics.sparktk.graph.Graph
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class ClosenessCentralityTest extends TestingSparkContextWordSpec with Matchers {

  "Closeness Centrality" should {
    def getGraph: Graph = {
      val sqlContext: SQLContext = new SQLContext(sparkContext)
      // Vertex DataFrame
      val v = sqlContext.createDataFrame(List(("a", "Ben"),
        ("b", "Anna"),
        ("c", "Cara"),
        ("d", "Dana"),
        ("e", "Evan"),
        ("f", "Frank"))).toDF("id", "name")
      val e = sqlContext.createDataFrame(List(
        ("a", "b", 1800.0),
        ("b", "c", 800.0),
        ("c", "d", 600.0),
        ("c", "e", 900.0),
        ("d", "e", 1100.0),
        ("d", "f", 700.0),
        ("e", "f", 500.0))).toDF("src", "dst", "distance")
      // create sparktk graph
      new Graph(v, e)
    }

    "calculate the closeness centrality with normalized values" in {
      val closenessCentralityFrame = getGraph.closenessCentrality()
      closenessCentralityFrame.collect().head.getDouble(2) shouldBe (0.4 +- 1E-6)
    }

    "calculate the closeness centrality" in {
      val closenessCentralityFrame = getGraph.closenessCentrality(None, normalize = false)
      closenessCentralityFrame.collect().head.getDouble(2) shouldBe (0.5 +- 1E-6)
    }

    "calculate the normalized closeness centrality with edge weights" in {
      val closenessCentralityFrame = getGraph.closenessCentrality(Some("distance"))
      closenessCentralityFrame.collect().head.getDouble(2) shouldBe (5.333333333333334E-4 +- 1E-6)
    }

    "calculate the closeness centrality with edge weights" in {
      val closenessCentralityFrame = getGraph.closenessCentrality(Some("distance"), normalize = false)
      closenessCentralityFrame.collect().head.getDouble(2) shouldBe (6.666666666666666E-4 +- 1E-6)
    }
  }
}
