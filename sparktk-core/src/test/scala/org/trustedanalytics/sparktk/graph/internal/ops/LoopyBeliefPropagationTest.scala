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

import org.scalatest.{ WordSpec, Matchers }
import org.trustedanalytics.sparktk.frame.{ Frame, DataTypes, Column, FrameSchema }
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.trustedanalytics.sparktk.graph.{ Graph }
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class LoopyBeliefPropagationTest extends TestingSparkContextWordSpec with Matchers {

  private def buildGraph(): Graph = {
    val vertices = FrameSchema(List(Column("id", DataTypes.int32), Column("priors", DataTypes.string)))
    // This graph is a diamond, 1 to 3 and 4, 3 and 4 to 5
    val vertexRows = FrameRdd.toRowRDD(vertices, sparkContext.parallelize(List(Array[Any](1, "1.0 0"), Array[Any](3, "1.0 0"), Array[Any](4, "1.0 0"), Array[Any](5, "0 1.0"))))
    val edges = FrameSchema(List(Column("src", DataTypes.int32), Column("dst", DataTypes.int32), Column("weights", DataTypes.float32)))
    val edgeRows = FrameRdd.toRowRDD(edges, sparkContext.parallelize(
      List(Array[Any](1, 3, 1),
        Array[Any](1, 4, 1),
        Array[Any](3, 5, 1),
        Array[Any](4, 5, 1),
        Array[Any](3, 4, 1))))

    val edgeFrame = new Frame(edgeRows, edges)
    val vertexFrame = new Frame(vertexRows, vertices)
    val graph = new Graph(vertexFrame, edgeFrame)
    graph
  }

  "loopy belief propgation" when {
    "called with good inputs" should {

      "optimize over the priors" in {
        val graph = buildGraph()
        val posteriorBelief = graph.loopyBeliefPropagation("priors", "weights")
        posteriorBelief.schema.columns should equal(List(Column("id", DataTypes.int32), Column("priors", DataTypes.string), Column("posterior", DataTypes.string)))
        posteriorBelief.rdd.toArray.toList should equal(List(
          new GenericRow(Array[Any](1, "1.0 0", "[1.0,0.0]")),
          new GenericRow(Array[Any](3, "1.0 0", "[1.0,0.0]")),
          new GenericRow(Array[Any](5, "0 1.0", "[0.0,1.0]")),
          new GenericRow(Array[Any](4, "1.0 0", "[1.0,0.0]"))))

      }
    }

    "called with invalid prior" should {
      "throw an exception" in {
        val graph = buildGraph()
        val thrown = the[IllegalArgumentException] thrownBy graph.loopyBeliefPropagation("invalid_prior", "weights")
        thrown.getMessage should equal("requirement failed: Property invalid_prior not found for prior")
      }
    }

    "called with an invalid weight" should {
      "throw an exception" in {
        val graph = buildGraph()
        val thrown = the[IllegalArgumentException] thrownBy graph.loopyBeliefPropagation("prior", "invalid_prior")
        thrown.getMessage should equal("requirement failed: Property invalid_prior not found for edge weight")
      }
    }

    "called with invalid iterations" should {
      "throw an exception" in {
        val graph = buildGraph()
        val thrown = the[IllegalArgumentException] thrownBy graph.loopyBeliefPropagation("prior", "invalid_prior", -3)
        thrown.getMessage should equal("requirement failed: maxIterations must be greater than 0")
      }
    }
  }
}
