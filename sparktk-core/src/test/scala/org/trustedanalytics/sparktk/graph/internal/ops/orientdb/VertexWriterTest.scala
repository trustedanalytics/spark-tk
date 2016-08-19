package org.trustedanalytics.sparktk.graph.internal.ops.orientdb

import org.apache.spark.sql.SQLContext
import org.graphframes.GraphFrame
import org.scalatest.{ WordSpec, Matchers, BeforeAndAfterEach }
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class VertexWriterTest extends WordSpec with Matchers with TestingOrientDb with TestingSparkContextWordSpec with BeforeAndAfterEach {

  override def beforeEach() {
    setupOrientDbInMemory()
  }

  override def afterEach() {
    cleanupOrientDbInMemory()
  }

  "vertex writer" should {
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
        ("a", "g", "friend"),
        ("l", "a", "follow"))).toDF("src", "dst", "relationship")
      // Create a org.graphframes.GraphFrame
      GraphFrame(v, e)
    }
    "create OrientDB vertex" in {
      val vertexWriter = new VertexWriter(orientMemoryGraph)
      friends.vertices.collect().foreach(row => {
        //method under test
        vertexWriter.create(verticesClassName, row)
      })
      //validate the results
      val namePropValue: Any = orientMemoryGraph.getVertices("id_", "a").iterator().next().getProperty("name")
      val agePropValue: Any = orientMemoryGraph.getVertices("id_", "a").iterator().next().getProperty("age")
      assert(namePropValue == "Alice")
      assert(agePropValue == 34)
    }

    "find a vertex" in {
      val schemaWriter = new SchemaWriter(orientMemoryGraph)
      schemaWriter.vertexSchema(friends.vertices.schema, verticesClassName)
      val vertexWriter = new VertexWriter(orientMemoryGraph)
      friends.vertices.collect().foreach(row => {
        vertexWriter.create(verticesClassName, row)
      })
      //method under test
      val vertex = vertexWriter.find("a", verticesClassName)
      //validate the results
      val agePropValue: Any = orientMemoryGraph.getVertices("id_", "a").iterator().next().getProperty("age")
      assert(agePropValue == 34)
    }

    "find or creates a vertex" in {
      val vertexWriter = new VertexWriter(orientMemoryGraph)
      friends.vertices.collect().foreach(row => {
        vertexWriter.create(verticesClassName, row)
      })
      //method under test
      val vertexFound = vertexWriter.findOrCreate("a", verticesClassName)
      val vertexCreated = vertexWriter.findOrCreate("z", verticesClassName)
      //validate the results
      val agePropValue: Any = orientMemoryGraph.getVertices("id_", "a").iterator().next().getProperty("age")
      assert(agePropValue == 34)
      assert(orientMemoryGraph.getVertices("id_", "z").iterator().hasNext)
    }
    "update a vertex" in {
      // load the old graph frame "friends"
      val vertexWriter = new VertexWriter(orientMemoryGraph)
      friends.vertices.collect.foreach(row => {
        vertexWriter.create(verticesClassName, row)
      })
      // Load the new graph frame to the same class "friends"
      newGraphFrame.vertices.collect.foreach(row => {
        //method under test
        vertexWriter.updateOrCreate(row, verticesClassName)
      })
      // validating results
      val namePropValue: Any = orientMemoryGraph.getVertices("id_", "a").iterator().next().getProperty("name")
      val agePropValue: Any = orientMemoryGraph.getVertices("id_", "a").iterator().next().getProperty("age")
      val namePropValue2: Any = orientMemoryGraph.getVertices("id_", "l").iterator().next().getProperty("name")
      assert(namePropValue == "Gabby")
      assert(agePropValue == 60)
      assert(namePropValue2 == "Lyla")
    }

  }

}
