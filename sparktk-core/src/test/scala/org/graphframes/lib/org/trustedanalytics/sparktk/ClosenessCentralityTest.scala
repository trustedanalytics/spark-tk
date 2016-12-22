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

import org.apache.spark.graphx.lib.org.trustedanalytics.sparktk.ClosenessCalculations
import org.apache.spark.sql.SQLContext
import org.graphframes._
import org.scalatest.Matchers
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class ClosenessCentralityTest extends TestingSparkContextWordSpec with Matchers {

  "Single source shortest path" should {
    def getGraph: GraphFrame = {
      val sqlContext: SQLContext = new SQLContext(sparkContext)
      // Vertex DataFrame
      val v = sqlContext.createDataFrame(List((1L, "Ben"),
        (2L, "Anna"),
        (3L, "Cara"),
        (4L, "Dana"),
        (5L, "Evan"),
        (6L, "Frank"))).toDF("id", "name")
      val e = sqlContext.createDataFrame(List(
        (1L, 2L, 1800.0),
        (2L, 3L, 800.0),
        (3L, 4L, 600.0),
        (3L, 5L, 900.0),
        (4L, 5L, 1100.0),
        (4L, 6L, 700.0),
        (5L, 6L, 500.0))).toDF("src", "dst", "distance")
      // Create a GraphFrame
      GraphFrame(v, e)
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
      val closenessCentrality = ClosenessCentrality.run(getGraph, Some("distance"))
      assert(closenessCentrality(3) == ClosenessCalculations(3, 6.428571428571428E-4))
    }

    "calculate the closeness centrality with edge weights" in {
      val closenessCentrality = ClosenessCentrality.run(getGraph, Some("distance"), normalized = false)
      assert(closenessCentrality(3) == ClosenessCalculations(3, 0.0010714285714285715))
    }
  }
}
