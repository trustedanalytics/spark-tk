package org.trustedanalytics.sparktk.graph.internal.ops

import org.graphframes.examples
import org.scalatest.Matchers
import org.trustedanalytics.sparktk.graph.Graph
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class ShortestPathTest extends TestingSparkContextWordSpec with Matchers {

  "Shortest Path Algorithm" should {

    " calculate shortest paths in GraphFrames" in {
      val friends = examples.Graphs.friends
      val v = friends.shortestPaths.landmarks(Seq("a","d")).run()
    }

    "calculate shortest paths in Spark-Tk" in {
      val friends = examples.Graphs.friends
      val graph = new Graph(friends)
      val sp = graph.shortestPath(Seq("a","d")).collect()
    }
  }
}
