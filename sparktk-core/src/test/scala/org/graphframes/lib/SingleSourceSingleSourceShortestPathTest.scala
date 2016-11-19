package org.graphframes.lib

import org.apache.spark.sql.{SQLContext, Row}
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
