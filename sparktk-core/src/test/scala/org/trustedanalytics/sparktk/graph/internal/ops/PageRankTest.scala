package org.trustedanalytics.sparktk.graph.internal.ops

import org.scalatest.{ WordSpec, Matchers }
import org.trustedanalytics.sparktk.frame.{ Frame, DataTypes, Column, FrameSchema }
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.trustedanalytics.sparktk.graph.{ Graph }
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class PageRankTest extends TestingSparkContextWordSpec with Matchers {

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

  "page rank" when {
    "set with max iterations" should {
      "calculate the page rank" in {
        val graph = buildGraph()
        val pageRank = graph.pageRank(Some(20), None, None)
        pageRank.schema.columns should equal(List(Column("Vertex", DataTypes.int32), Column("PageRank", DataTypes.float64)))
        pageRank.rdd.toArray.toList should contain theSameElementsAs List(new GenericRow(Array[Any](1, 0.15)), new GenericRow(Array[Any](3, 0.21375)), new GenericRow(Array[Any](5, 0.513375)), new GenericRow(Array[Any](4, 0.21375)))

      }
    }

    "set with convergence tolerance" should {
      "calculate the page rank" in {
        val graph = buildGraph()
        val pageRank = graph.pageRank(None, None, Some(0.001))
        pageRank.schema.columns should equal(List(Column("Vertex", DataTypes.int32), Column("PageRank", DataTypes.float64)))
        pageRank.rdd.toArray.toList should contain theSameElementsAs List(new GenericRow(Array[Any](1, 0.15)), new GenericRow(Array[Any](3, 0.21375)), new GenericRow(Array[Any](5, 0.513375)), new GenericRow(Array[Any](4, 0.21375)))

      }
    }

    "called without convergence tolerance or pagerank" should {
      "throw an exception" in {
        val graph = buildGraph()
        val thrown = the[IllegalArgumentException] thrownBy graph.pageRank(None, None, None)
        thrown.getMessage should equal("requirement failed: Must set one of max iterations or convergence tolerance")
      }
    }

    "called with convergence tolerance and pagerank" should {
      "throw an exception" in {
        val graph = buildGraph()
        val thrown = the[IllegalArgumentException] thrownBy graph.pageRank(Some(20), None, Some(0.001))
        thrown.getMessage should equal("requirement failed: Only set one of max iterations or convergence tolerance")
      }
    }

  }
}
