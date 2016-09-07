package org.trustedanalytics.sparktk.graph.internal.ops

import org.scalatest.{ WordSpec, Matchers }
import org.trustedanalytics.sparktk.frame.{ Frame, DataTypes, Column, FrameSchema }
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.trustedanalytics.sparktk.graph.{ Graph }
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class DegreeTest extends TestingSparkContextWordSpec with Matchers {

  private def buildGraph(): Graph = {
    val vertices = FrameSchema(List(Column("id", DataTypes.int32)))
    // This graph is a diamond, 1 to 3 and 4, 3 and 4 to 5
    val vertexRows = FrameRdd.toRowRDD(vertices, sparkContext.parallelize(List(Array[Any](1), Array[Any](3), Array[Any](4), Array[Any](5))))
    val edges = FrameSchema(List(Column("src", DataTypes.int32), Column("dst", DataTypes.int32)))
    val edgeRows = FrameRdd.toRowRDD(edges, sparkContext.parallelize(
      List(Array[Any](1, 3),
        Array[Any](1, 4),
        Array[Any](3, 5),
        Array[Any](4, 5))))

    val edgeFrame = new Frame(edgeRows, edges)
    val vertexFrame = new Frame(vertexRows, vertices)
    val graph = new Graph(vertexFrame, edgeFrame)
    graph
  }

  "degree" when {
    "set with indegree" should {
      "give the indegree" in {
        val graph = buildGraph()
        val degree = graph.degree("in")
        degree.schema.columns should equal(List(Column("Vertex", DataTypes.int32), Column("Degree", DataTypes.int64)))
        degree.rdd.toArray.toList should contain theSameElementsAs List(new GenericRow(Array[Any](1, 0)), new GenericRow(Array[Any](3, 1)), new GenericRow(Array[Any](5, 2)), new GenericRow(Array[Any](4, 1)))

      }
    }

    "set with outdegree" should {
      "give the outdegree" in {
        val graph = buildGraph()
        val degree = graph.degree("out")
        degree.schema.columns should equal(List(Column("Vertex", DataTypes.int32), Column("Degree", DataTypes.int64)))
        degree.rdd.toArray.toList should contain theSameElementsAs List(new GenericRow(Array[Any](1, 2)), new GenericRow(Array[Any](3, 1)), new GenericRow(Array[Any](5, 0)), new GenericRow(Array[Any](4, 1)))

      }
    }

    "set with undirected" should {
      "sum the properties going in or out" in {
        val graph = buildGraph()
        val degree = graph.degree("undirected")
        degree.schema.columns should contain theSameElementsInOrderAs List(Column("Vertex", DataTypes.int32), Column("Degree", DataTypes.int64))
        degree.rdd.toArray.toList should contain theSameElementsAs List(new GenericRow(Array[Any](1, 2)), new GenericRow(Array[Any](3, 2)), new GenericRow(Array[Any](5, 2)), new GenericRow(Array[Any](4, 2)))

      }
    }

    "called with an invalid string" should {
      "throw an exception" in {
        val graph = buildGraph()
        val thrown = the[IllegalArgumentException] thrownBy graph.degree("invalid")
        thrown.getMessage should equal("requirement failed: Invalid degree option, please choose \"in\", \"out\", or \"undirected\"")
      }
    }

  }
}
