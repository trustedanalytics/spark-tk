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
package org.graphframes.lib

import org.apache.spark.sql.{ SQLContext, Row }
import org.apache.spark.sql.types.DataTypes
import org.graphframes._
import org.scalatest.Matchers
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class SingleSourceSingleSourceShortestPathTest extends TestingSparkContextWordSpec with Matchers {

  "SSSP" should {
    "friends graph example" in {
      val friends = examples.Graphs.friends
      val v = friends.shortestPaths.landmarks(Seq("a", "d")).run()
      val expected = Set[(String, Map[String, Int])](("a", Map("a" -> 0, "d" -> 2)), ("b", Map.empty),
        ("c", Map.empty), ("d", Map("a" -> 1, "d" -> 0)), ("e", Map("a" -> 2, "d" -> 1)),
        ("f", Map.empty), ("g", Map.empty))
      val results = v.select("id", "distances").collect().map {
        case Row(id: String, spMap: Map[String, Int] @unchecked) =>
          (id, spMap)
      }.toSet
      assert(results === expected)
    }

  }
}
