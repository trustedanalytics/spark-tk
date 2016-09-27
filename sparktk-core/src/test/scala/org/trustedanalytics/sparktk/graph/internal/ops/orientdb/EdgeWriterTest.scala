package org.trustedanalytics.sparktk.graph.internal.ops.orientdb

import org.apache.spark.sql.SQLContext
import org.graphframes.GraphFrame
import org.scalatest.{ BeforeAndAfterEach, WordSpec }
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class EdgeWriterTest extends WordSpec with TestingOrientDb with TestingSparkContextWordSpec with BeforeAndAfterEach {
  override def beforeEach() {
    setupOrientDb()
  }

  override def afterEach() {
    cleanupOrientDb()
  }

  "Edge writer" should {
    //create Graph of friends in a social network.
    def friends: GraphFrame = {
      val sqlContext: SQLContext = new SQLContext(sparkContext)
      // Vertex DataFrame
      val v = sqlContext.createDataFrame(List(
        ("a", "Alice", 34, "type_A"),
        ("b", "Bob", 36, "type_B"))).toDF("id", "name", "age", "type")
      // Edge DataFrame
      val e = sqlContext.createDataFrame(List(
        ("a", "b", "friend"),
        ("b", "c", "follow")
      )).toDF("src", "dst", "relationship")
      // Create a org.graphframes.GraphFrame
      GraphFrame(v, e)
    }

    "create edge" in {
      val schemaWriter = new SchemaWriter
      schemaWriter.vertexSchema(friends.vertices, orientFileGraph)
      schemaWriter.edgeSchema(friends.edges, orientFileGraph)
      val vertexFrameWriter = new VertexFrameWriter(friends.vertices, dbConfig)
      vertexFrameWriter.exportVertexFrame(dbConfig.batchSize)
      friends.edges.collect().foreach(row => {
        val edgeWriter = new EdgeWriter(orientFileGraph)
        //call method under test
        edgeWriter.create(row)
      })
      //validate the results
      val relPropValue: Any = orientFileGraph.getEdges("src", "a").iterator().next().getProperty("relationship")
      assert(relPropValue == "friend")
    }
  }

}
