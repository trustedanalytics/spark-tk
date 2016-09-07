package org.trustedanalytics.sparktk.graph.internal.ops

import org.scalatest.{ WordSpec, Matchers }
import org.trustedanalytics.sparktk.frame.{ Frame, DataTypes, Column, FrameSchema }
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.trustedanalytics.sparktk.graph.{ Graph }
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class TriangleCountTest extends TestingSparkContextWordSpec with Matchers {

  private def buildGraph(): Graph = {
    val vertices = FrameSchema(List(Column("id", DataTypes.int32)))
    // This graph is a diamond, 1 to 3 and 4, 3 and 4 to 5
    val vertexRows = FrameRdd.toRowRDD(vertices, sparkContext.parallelize(List(Array[Any](1), Array[Any](3), Array[Any](4), Array[Any](5))))
    val edges = FrameSchema(List(Column("src", DataTypes.int32), Column("dst", DataTypes.int32)))
    val edgeRows = FrameRdd.toRowRDD(edges, sparkContext.parallelize(
      List(Array[Any](1, 3),
        Array[Any](1, 4),
        Array[Any](3, 5),
        Array[Any](4, 5),
        Array[Any](3, 4))))

    val edgeFrame = new Frame(edgeRows, edges)
    val vertexFrame = new Frame(vertexRows, vertices)
    val graph = new Graph(vertexFrame, edgeFrame)
    graph
  }

  "triangle count" in {
    val graph = buildGraph()
    val weightedDegree = graph.triangleCount()
    weightedDegree.schema.columns should equal(List(Column("Triangles", DataTypes.int64), Column("Vertex", DataTypes.int32)))
    weightedDegree.rdd.toArray.toList should equal(List(new GenericRow(Array[Any](1, 1)), new GenericRow(Array[Any](2, 3)), new GenericRow(Array[Any](1, 5)), new GenericRow(Array[Any](2, 4))))

  }
}
