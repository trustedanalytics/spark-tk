package org.trustedanalytics.sparktk.graph.internal.constructors.fromorientdb

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.graphframes.GraphFrame
import org.scalatest.{ BeforeAndAfterEach, WordSpec }
import org.trustedanalytics.sparktk.graph.internal.ops.orientdb.{ TestingOrientDb, SchemaWriter }
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class SchemaReaderTest extends WordSpec with TestingOrientDb with TestingSparkContextWordSpec with BeforeAndAfterEach {

  override def beforeEach() {
    setupOrientDbInMemory()
    def friends: GraphFrame = {
      val sqlContext: SQLContext = new SQLContext(sparkContext)
      // Vertex DataFrame
      val v = sqlContext.createDataFrame(List(
        ("g", "Gabby", 60))).toDF("id", "name", "age")
      // Edge DataFrame
      val e = sqlContext.createDataFrame(List(
        ("a", "e", "friend"))).toDF("src", "dst", "relationship")
      GraphFrame(v, e)
    }
    val schemaWriter = new SchemaWriter(orientMemoryGraph)
    schemaWriter.vertexSchema(friends.vertices.schema, verticesClassName)
    schemaWriter.edgeSchema(friends.edges.schema)
  }

  override def afterEach() {
    cleanupOrientDbInMemory()
  }

  "schema reader" should {
    "import vertex schema" in {
      val schemaReader = new SchemaReader(orientMemoryGraph)
      // call method under test
      val vertexSchema = schemaReader.importVertexSchema(verticesClassName)
      // validate results
      assert(vertexSchema.fieldNames.toList == List("id", "name", "age"))
      assert(vertexSchema.fields.apply(0).dataType == StringType)
      assert(vertexSchema.fields.apply(1).dataType == StringType)
      assert(vertexSchema.fields.apply(2).dataType == IntegerType)
    }
    "import edge schema" in {
      val schemaReader = new SchemaReader(orientMemoryGraph)
      //call method under test
      val edgeSchema = schemaReader.importEdgeSchema
      // validate results
      assert(edgeSchema.fieldNames.toList == List("dst", "src", "relationship"))
      assert(edgeSchema.fields.apply(0).dataType == StringType)
      assert(edgeSchema.fields.apply(1).dataType == StringType)
      assert(edgeSchema.fields.apply(2).dataType == StringType)
    }
  }

}
