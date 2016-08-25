package org.trustedanalytics.sparktk.graph.internal.ops

import org.scalatest.{ WordSpec, Matchers }
import org.trustedanalytics.sparktk.frame.{ Frame, DataTypes, Column, FrameSchema }
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.trustedanalytics.sparktk.graph.{ Graph }
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class WeightedDegreeTest extends TestingSparkContextWordSpec with Matchers {

  private def buildGraph(): Graph = {
    val vertices = FrameSchema(List(Column("id", DataTypes.int32)))
    // This graph is a diamond, 1 to 3 and 4, 3 and 4 to 5
    val vertexRows = FrameRdd.toRowRDD(vertices, sparkContext.parallelize(List(Array[Any](1), Array[Any](3), Array[Any](4), Array[Any](5))))
    val edges = FrameSchema(List(Column("src", DataTypes.int32), Column("dst", DataTypes.int32), Column("count", DataTypes.int32)))
    val edgeRows = FrameRdd.toRowRDD(edges, sparkContext.parallelize(
      List(Array[Any](1, 3, 2),
        Array[Any](1, 4, 3),
        Array[Any](3, 5, 7),
        Array[Any](4, 5, 11))))

    val edgeFrame = new Frame(edgeRows, edges)
    val vertexFrame = new Frame(vertexRows, vertices)
    val graph = new Graph(vertexFrame, edgeFrame)
    graph
  }

  "weighted degree" when {
    "set with outdegree" should {
      "sum the properties going in" in {
        val graph = buildGraph()
        val weightedDegree = graph.weightedDegree("count", "in", 0)
        weightedDegree.schema.columns should equal(List(Column("Vertex", DataTypes.int32), Column("Degree", DataTypes.int64)))
        weightedDegree.rdd.collect() should equal(List(new GenericRow(Array[Any](1, 0)), new GenericRow(Array[Any](3, 2)), new GenericRow(Array[Any](5, 18)), new GenericRow(Array[Any](4, 3))))

      }
    }

    "set with outdegree" should {
      "sum the properties going out" in {
        val graph = buildGraph()
        val weightedDegree = graph.weightedDegree("count", "out", 0)
        weightedDegree.schema.columns should equal(List(Column("Vertex", DataTypes.int32), Column("Degree", DataTypes.int64)))
        weightedDegree.rdd.collect() should equal(List(new GenericRow(Array[Any](1, 5)), new GenericRow(Array[Any](3, 7)), new GenericRow(Array[Any](5, 0)), new GenericRow(Array[Any](4, 11))))

      }
    }

    "set with undirected" should {
      "sum the properties going in or out" in {
        val graph = buildGraph()
        val weightedDegree = graph.weightedDegree("count", "undirected", 0)
        weightedDegree.schema.columns should contain theSameElementsInOrderAs List(Column("Vertex", DataTypes.int32), Column("Degree", DataTypes.int64))
        weightedDegree.rdd.collect() should contain theSameElementsAs List(new GenericRow(Array[Any](1, 5)), new GenericRow(Array[Any](3, 9)), new GenericRow(Array[Any](5, 18)), new GenericRow(Array[Any](4, 14)))

      }
    }

    "called with an invalid string" should {
      "throw an exception" in {
        val graph = buildGraph()
        val thrown = the[IllegalArgumentException] thrownBy graph.weightedDegree("count", "invalid", 0)
        thrown.getMessage should equal("requirement failed: Invalid degree option, please choose \"in\", \"out\", or \"undirected\"")
      }
    }

  }
}
