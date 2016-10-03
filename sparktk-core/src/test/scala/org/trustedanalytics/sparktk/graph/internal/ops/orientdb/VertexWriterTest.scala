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
        ("b", "Bob", 36))).toDF("id", "name", "age")
      // Edge DataFrame
      val e = sqlContext.createDataFrame(List(
        ("a", "b", "friend"),
        ("b", "c", "follow"))).toDF("src", "dst", "relationship")
      // Create a org.graphframes.GraphFrame
      GraphFrame(v, e)
    }
    "create OrientDB vertex" in {
      orientMemoryGraph.makeActive()
      val schemaWriter = new SchemaWriter
      schemaWriter.vertexSchema(friends.vertices, orientMemoryGraph)
      val vertexWriter = new VertexWriter(orientMemoryGraph)
      friends.vertices.collect().foreach(row => {
        //method under test
        vertexWriter.create(row)
      })
      //validate the results
      val namePropValue: Any = orientMemoryGraph.getVertices("id_", "a").iterator().next().getProperty("name")
      val agePropValue: Any = orientMemoryGraph.getVertices("id_", "a").iterator().next().getProperty("age")
      assert(namePropValue == "Alice")
      assert(agePropValue == 34)
    }

    "find a vertex" in {
      orientMemoryGraph.makeActive()
      val schemaWriter = new SchemaWriter
      schemaWriter.vertexSchema(friends.vertices, orientMemoryGraph)
      val vertexWriter = new VertexWriter(orientMemoryGraph)
      friends.vertices.collect().foreach(row => {
        vertexWriter.create(row)
      })
      //method under test
      val vertex = vertexWriter.find("a")
      //validate the results
      assert(vertex.get.getProperty[Int]("age") == 34)
    }
  }

}
