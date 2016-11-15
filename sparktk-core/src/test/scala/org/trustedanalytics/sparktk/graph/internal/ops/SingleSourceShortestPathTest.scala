package org.trustedanalytics.sparktk.graph.internal.ops

import org.graphframes.examples
import org.scalatest.Matchers
import org.trustedanalytics.sparktk.graph.Graph
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class SingleSourceShortestPathTest extends TestingSparkContextWordSpec with Matchers {

  "Single Source Shortest Path Algorithm" should {

    "Shortest paths in GraphFrames" in {
      val friends = examples.Graphs.friends
      val v = friends.shortestPaths.landmarks(Seq("a","d")).run()
    }

    "Shortest paths in Spark-Tk" in {
      val friends = examples.Graphs.friends
      val graph = new Graph(friends)
      val sp = graph.shortestPath(Seq("a","d")).collect()
    }
  }

}
