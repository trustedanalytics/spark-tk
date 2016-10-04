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
package org.trustedanalytics.sparktk.graph.internal.constructors.fromorientdb

import org.apache.spark.sql.{ Row, SQLContext }
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
      val schema = StructType(List(StructField("id", StringType),
        StructField("name", StringType),
        StructField("age", IntegerType)))
      val rowRdd = sparkContext.parallelize(List(Row.fromTuple(("g", "Gabby", 60)),
        Row.fromTuple(("c", "Roby", 60)),
        Row.fromTuple(("b", "Bob", 30))))
      val v = sqlContext.createDataFrame(rowRdd, schema)
      // Edge Dataframe
      val edgeSchema = StructType(List(StructField("src", StringType),
        StructField("dst", StringType),
        StructField("relationship", StringType)))
      val edgeRowRdd = sparkContext.parallelize(List(Row.fromTuple(("a", "e", "friend"))))
      val e = sqlContext.createDataFrame(edgeRowRdd, edgeSchema)
      GraphFrame(v, e)
    }
    val schemaWriter = new SchemaWriter
    schemaWriter.vertexSchema(friends.vertices, orientMemoryGraph)
    schemaWriter.edgeSchema(friends.edges, orientMemoryGraph)
  }

  override def afterEach() {
    cleanupOrientDbInMemory()
  }

  "schema reader" should {
    "import vertex schema" in {
      val schemaReader = new SchemaReader(orientMemoryGraph)
      // call method under test
      val vertexSchema = schemaReader.importVertexSchema
      // validate results
      assert(vertexSchema.fieldNames.toList == List("id", "name", "age"))
      assert(vertexSchema.fields.apply(0).dataType == StringType)
      assert(vertexSchema.fields.apply(1).dataType == StringType)
      assert(vertexSchema.fields.apply(2).dataType == IntegerType)
    }

    "validate vertex schema should throw exception on schemas missing required column names" in {
      // orientMemoryGraph.makeActive()
      val badVertexSchema = StructType(Array(StructField("id_", StringType)))
      val schemaReader = new SchemaReader(orientMemoryGraph)
      intercept[IllegalArgumentException] {
        //call method under test
        schemaReader.validateVertexSchema(badVertexSchema)
      }
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

    "validate edge schema should throw exception on schemas missing required column names" in {
      val badEdgeSchema = StructType(Array(StructField("src_vertex", StringType),
        StructField("dst_vertex", StringType)))
      val schemaReader = new SchemaReader(orientMemoryGraph)
      intercept[IllegalArgumentException] {
        //call method under test
        schemaReader.validateEdgeSchema(badEdgeSchema)
      }
    }
  }

}
