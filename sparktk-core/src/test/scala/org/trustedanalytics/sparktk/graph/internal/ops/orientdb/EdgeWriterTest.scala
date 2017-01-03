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
package org.trustedanalytics.sparktk.graph.internal.ops.orientdb

import org.apache.spark.sql.{ Row, SQLContext }
import org.apache.spark.sql.types.{ IntegerType, StringType, StructField, StructType }
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
      val schema = StructType(List(StructField("id", StringType),
        StructField("name", StringType),
        StructField("age", IntegerType),
        StructField("type", StringType)))
      val rowRdd = sparkContext.parallelize(List(Row.fromTuple(("a", "Alice", 34, "type_A")),
        Row.fromTuple(("b", "Bob", 36, "type_B"))))
      val v = sqlContext.createDataFrame(rowRdd, schema)
      // Edge Dataframe
      val edgeSchema = StructType(List(StructField("src", StringType),
        StructField("dst", StringType),
        StructField("relationship", StringType)))
      val edgeRowRdd = sparkContext.parallelize(List(Row.fromTuple(("a", "b", "friend")),
        Row.fromTuple(("b", "c", "follow"))))
      val e = sqlContext.createDataFrame(edgeRowRdd, edgeSchema)
      // Create a org.graphframes.GraphFrame
      GraphFrame(v, e)
    }

    "create edge" in {
      val schemaWriter = new SchemaWriter
      // export vertices
      schemaWriter.vertexSchema(friends.vertices, orientFileGraph)
      val vertexWriter = new VertexWriter(orientFileGraph)
      friends.vertices.collect().foreach(row => {
        vertexWriter.create(row)
      })
      // export edges
      schemaWriter.edgeSchema(friends.edges, orientFileGraph)
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
