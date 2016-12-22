package org.apache.spark.graphx.lib.org.trustedanalytics.sparktk

import org.apache.spark.graphx.{Edge, Graph}
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

    "calculate the single source shortest path" in {
      val closenessCentrality = ClosenessCentrality.run(getGraph,None, None,true)
      println(closenessCentrality.mkString("\n"))
    }
  }
}
