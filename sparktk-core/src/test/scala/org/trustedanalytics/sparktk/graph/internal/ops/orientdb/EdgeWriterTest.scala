package org.trustedanalytics.sparktk.graph.internal.ops.orientdb

import com.orientechnologies.orient.core.metadata.schema.OType
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

    //create Graph of friends in a social network.
    def newGraphFrame: GraphFrame = {
      val sqlContext: SQLContext = new SQLContext(sparkContext)
      // Vertex DataFrame
      val v = sqlContext.createDataFrame(List(
        ("g", "Alice", 34, "type_A"),
        ("a", "Gabby", 60, "type_B"),
        ("l", "Lyla", 20, "type_C"))).toDF("id", "name", "age", "type")
      // Edge DataFrame
      val e = sqlContext.createDataFrame(List(
        ("a", "b", "follow"),
        ("l", "a", "follow"))).toDF("src", "dst", "relationship")
      // Create a org.graphframes.GraphFrame
      GraphFrame(v, e)
    }
    /*"create edge" in {
      val schemaWriter = new SchemaWriter(orientFileGraph)
      schemaWriter.vertexSchema(friends.vertices)
      schemaWriter.edgeSchema(friends.edges.schema)
      val vertexFrameWriter = new VertexFrameWriter(friends.vertices, dbConfig)
      vertexFrameWriter.exportVertexFrame(dbConfig.batchSize)
      friends.edges.collect().foreach(row => {
        val edgeWriter = new EdgeWriter(orientFileGraph)
        //call method under test
        edgeWriter.create(row, verticesClassName)
      })
      //validate the results
      val relPropValue: Any = orientFileGraph.getEdges("src", "a").iterator().next().getProperty("relationship")
      assert(relPropValue == "friend")
    }

    "find edge" in {
      val schemaWriter = new SchemaWriter(orientFileGraph)
      schemaWriter.vertexSchema(friends.vertices)
      val edgeType = schemaWriter.edgeSchema(friends.edges.schema)
      val vertexFrameWriter = new VertexFrameWriter(friends.vertices, dbConfig)
      vertexFrameWriter.exportVertexFrame(dbConfig.batchSize)
      val edgeFrameWriter = new EdgeFrameWriter(friends.edges, dbConfig)
      edgeFrameWriter.exportEdgeFrame(dbConfig.batchSize)
      val edgeWriter = new EdgeWriter(orientFileGraph)
      val row = friends.edges.collect.apply(0)
      //call method under test
      val foundEdge = edgeWriter.find(friends.edges.collect.apply(1)).get
      //validate results
      assert(edgeType.getProperty("relationship").getType == OType.STRING)
      assert(foundEdge.getLabel == "edge_")
      assert(foundEdge.getProperty[String]("relationship") == "friend")
    }

    "update edge" in {
      val schemaWriter = new SchemaWriter(orientFileGraph)
      schemaWriter.vertexSchema(friends.vertices)
      val edgeType = schemaWriter.edgeSchema(friends.edges.schema)
      val vertexFrameWriter = new VertexFrameWriter(friends.vertices, dbConfig)
      vertexFrameWriter.exportVertexFrame(dbConfig.batchSize)
      val edgeFrameWriter = new EdgeFrameWriter(friends.edges, dbConfig)
      edgeFrameWriter.exportEdgeFrame(dbConfig.batchSize)
      val row = newGraphFrame.edges.collect.apply(0)
      // call method under test
      val edgeWriter = new EdgeWriter(orientFileGraph)
      val updatedEdge = edgeWriter.updateOrCreate(row, verticesClassName)
      // validate results
      assert(updatedEdge.getProperty[String]("relationship") == "follow")
    }*/
  }

}
