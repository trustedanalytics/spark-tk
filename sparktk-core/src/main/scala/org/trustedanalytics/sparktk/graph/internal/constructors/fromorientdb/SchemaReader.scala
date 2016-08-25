/**
 *  Copyright (c) 2015 Intel Corporation 
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

import com.tinkerpop.blueprints.impls.orient._
import org.apache.spark.sql.types.{ DataType, StructField, StructType }
import org.graphframes.GraphFrame
import org.trustedanalytics.sparktk.graph.internal.GraphSchema
import org.trustedanalytics.sparktk.graph.internal.ops.orientdb.DataTypesConverter

import scala.collection.mutable.ListBuffer

/**
 * converts the OrientDB graph schema to Spark graph frame schema
 *
 * @param graph OrientDB graph database
 */
class SchemaReader(graph: OrientGraphNoTx) {

  /**
   * imports vertex schema from OrientDB to Spark graph frame vertex schema
   *
   * @return Spark graph frame vertex schema
   */
  def importVertexSchema(className: String): StructType = {

    try {
      createVertexSchema(className)
    }
    catch {
      case e: Exception =>
        throw new RuntimeException(s"Unable to read vertex schema from OrientDB graph: ${e.getMessage}")
    }
  }

  /**
   * creates Spark graph frame vertex schema
   *
   * @param className OrientDB vertex class name
   * @return Spark graph frame vertex schema
   */
  def createVertexSchema(className: String): StructType = {
    val vertexSchemaFields = new ListBuffer[StructField]
    val propKeysIterator = graph.getRawGraph.getMetadata.getSchema.getClass(className).properties().iterator()
    while (propKeysIterator.hasNext) {
      val prop = propKeysIterator.next()
      val propKey = prop.getName
      val propType = prop.getType
      val columnType: DataType = DataTypesConverter.orientdbToSpark(propType)
      val field = if (propKey == graphParameters.orientVertexId) {
        new StructField(GraphFrame.ID, columnType)
      }
      else {
        new StructField(propKey, columnType)
      }
      vertexSchemaFields += field
    }
    new StructType(vertexSchemaFields.toArray)
  }

  /**
   * converts OrientDB edges schema to Spark graph frame edges schema
   *
   * @return Spark graph frame edges schema
   */
  def importEdgeSchema: StructType = {
    try {
      createEdgeSchema
    }
    catch {
      case e: Exception =>
        throw new RuntimeException(s"Unable to read edge schema from OrientDB graph: ${e.getMessage}")
    }
  }

  /**
   * creates Spark graph frame edges schema
   *
   * @return Spark graph frame edges schema
   */
  def createEdgeSchema: StructType = {

    val schemaFields = new ListBuffer[StructField]
    val propKeysIterator = graph.getRawGraph.getMetadata.getSchema.getClass(GraphSchema.edgeTypeColumnName).properties().iterator()
    while (propKeysIterator.hasNext) {
      val prop = propKeysIterator.next()
      val propKey = prop.getName
      val propType = prop.getType
      val columnType = DataTypesConverter.orientdbToSpark(propType)
      val field = if (propKey == GraphSchema.edgeTypeColumnName) {
        new StructField(GraphFrame.EDGE, columnType)
      }
      else {
        new StructField(propKey, columnType)
      }
      schemaFields += field
    }
    new StructType(schemaFields.toArray)
  }

}

object graphParameters{
  val orientVertexId = GraphFrame.ID + "_"
}
