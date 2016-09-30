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
        ("b", "Bob", 36))).toDF("id", "name", "age")
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
    "create edge" in {
      val schemaWriter = new SchemaWriter(orientFileGraph)
      schemaWriter.vertexSchema(friends.vertices.schema, verticesClassName)
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
      schemaWriter.vertexSchema(friends.vertices.schema, verticesClassName)
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
      schemaWriter.vertexSchema(friends.vertices.schema, verticesClassName)
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
    }
  }

}
