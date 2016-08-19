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
        ("a", "Alice", 34),
        ("b", "Bob", 36),
        ("c", "Charlie", 30),
        ("d", "David", 29),
        ("e", "Esther", 32),
        ("f", "Fanny", 36),
        ("g", "Gabby", 60))).toDF("id", "name", "age")
      // Edge DataFrame
      val e = sqlContext.createDataFrame(List(
        ("a", "b", "friend"),
        ("b", "c", "follow"),
        ("c", "b", "follow"),
        ("f", "c", "follow"),
        ("e", "f", "follow"),
        ("e", "d", "friend"),
        ("d", "a", "friend"),
        ("a", "e", "friend")
      )).toDF("src", "dst", "relationship")
      // Create a org.graphframes.GraphFrame
      GraphFrame(v, e)
    }

    //create Graph of friends in a social network.
    def newGraphFrame: GraphFrame = {
      val sqlContext: SQLContext = new SQLContext(sparkContext)
      // Vertex DataFrame
      val v = sqlContext.createDataFrame(List(
        ("g", "Alice", 34),
        ("a", "Gabby", 60),
        ("l", "Lyla", 20))).toDF("id", "name", "age")
      // Edge DataFrame
      val e = sqlContext.createDataFrame(List(
        ("a", "b", "follow"),
        ("l", "a", "follow"))).toDF("src", "dst", "relationship")
      // Create a org.graphframes.GraphFrame
      GraphFrame(v, e)
    }
    val batchSize = 1000
    "create edge" in {
      val schemaWriter = new SchemaWriter(orientFileGraph)
      schemaWriter.vertexSchema(friends.vertices.schema, verticesClassName)
      schemaWriter.edgeSchema(friends.edges.schema)
      val vertexFrameWriter = new VertexFrameWriter(friends.vertices, dbConfig)
      vertexFrameWriter.exportVertexFrame(batchSize)
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
      val dbConfig = new OrientConf(dbUri, dbUserName, dbPassword, rootPassword)
      val schemaWriter = new SchemaWriter(orientFileGraph)
      schemaWriter.vertexSchema(friends.vertices.schema, verticesClassName)
      val edgeType = schemaWriter.edgeSchema(friends.edges.schema)
      val vertexFrameWriter = new VertexFrameWriter(friends.vertices, dbConfig)
      vertexFrameWriter.exportVertexFrame(batchSize)
      val edgeFrameWriter = new EdgeFrameWriter(friends.edges, dbConfig)
      edgeFrameWriter.exportEdgeFrame(batchSize)
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
      val arguments = new OrientGraphWriterArgs(batchSize)
      val dbConfig = new OrientConf(dbUri, dbUserName, dbPassword, rootPassword)
      val schemaWriter = new SchemaWriter(orientFileGraph)
      schemaWriter.vertexSchema(friends.vertices.schema, verticesClassName)
      val edgeType = schemaWriter.edgeSchema(friends.edges.schema)
      val vertexFrameWriter = new VertexFrameWriter(friends.vertices, dbConfig)
      vertexFrameWriter.exportVertexFrame(batchSize)
      val edgeFrameWriter = new EdgeFrameWriter(friends.edges, dbConfig)
      edgeFrameWriter.exportEdgeFrame(batchSize)
      val row = newGraphFrame.edges.collect.apply(0)
      // call method under test
      val edgeWriter = new EdgeWriter(orientFileGraph)
      val updatedEdge = edgeWriter.updateOrCreate(row, verticesClassName)
      // validate results
      assert(updatedEdge.getProperty[String]("relationship") == "follow")
    }
  }

}
