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

import org.graphframes.examples
import org.scalatest.Matchers
import org.trustedanalytics.sparktk.graph.Graph
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class SingleSourceShortestPathTest extends TestingSparkContextWordSpec with Matchers {

  "Single Source Shortest Path Algorithm" should {

    "Shortest paths in GraphFrames" in {
      val friends = examples.Graphs.friends
      val v = friends.shortestPaths.landmarks(Seq("a", "d")).run()
    }

    "Shortest paths in Spark-Tk" in {
      val friends = examples.Graphs.friends
      val graph = new Graph(friends)
      val sp = graph.shortestPath(Seq("a", "d")).collect()
    }
  }

}
