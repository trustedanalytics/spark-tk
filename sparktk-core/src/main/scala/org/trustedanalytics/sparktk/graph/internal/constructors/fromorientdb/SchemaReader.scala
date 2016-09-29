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

import com.tinkerpop.blueprints.impls.orient._
import org.apache.spark.sql.types.{ DataType, StructField, StructType }
import org.graphframes.GraphFrame
import org.trustedanalytics.sparktk.graph.internal.GraphSchema
import org.trustedanalytics.sparktk.graph.internal.ops.orientdb.DataTypesConverter

import scala.collection.mutable.ListBuffer

/**
 * converts OrientDB graph schema to Spark graph frame schema
 * @param graph OrientDB graph database
 */
class SchemaReader(graph: OrientGraphNoTx) {

  /**
   * imports vertex schema from OrientDB to vertex data frame schema
   *
   * @return  vertex schema for spark data frame
   */
  def importVertexSchema: StructType = {

    try {
      val vertexSchema = createVertexSchema
      validateVertexSchema(vertexSchema)
      vertexSchema
    }
    catch {
      case e: Exception =>
        throw new RuntimeException(s"Unable to read vertex schema from OrientDB graph: ${e.getMessage}")
    }
  }

  /**
   * imports and validates OrientDB graph schema for vertices and converts it to vertex data frame schema
   *
   * @return vertex schema for spark data frame
   */
  def createVertexSchema: StructType = {

    // validate that OrientDB graph has the proper edge type name to be imported as a graph frame
    val classBaseNames = graph.getVertexBaseType.getName
    val className = graph.getVertexType(classBaseNames).getAllSubclasses.iterator().next().getName
    require(className == GraphSchema.vertexTypeColumnName, s"OrientDB graph must have single vertex type named ${GraphSchema.vertexTypeColumnName} to be imported as Graph Frame")
    // import properties
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
   * converts OrientDB edge schema to spark dataframe edge schema
   *
   * @return edge schema for spark data frame
   */
  def importEdgeSchema: StructType = {
    try {
      val edgeSchema = createEdgeSchema
      validateEdgeSchema(edgeSchema)
      edgeSchema
    }
    catch {
      case e: Exception =>
        throw new RuntimeException(s"Unable to read edge schema from OrientDB graph: ${e.getMessage}")
    }
  }

  /**
   * imports and validates OrientDB graph schema for edges and converts it to edge data frame schema
   *
   * @return edge schema for spark data frame
   */
  def createEdgeSchema: StructType = {

    // validate that OrientDB graph has the proper edge type name to be imported as a graph frame
    val classBaseNames = graph.getEdgeBaseType.getName
    require(graph.getEdgeType(classBaseNames).getAllSubclasses.iterator().next().getName == GraphSchema.edgeTypeColumnName, s"OrientDB graph must have single edge type named ${GraphSchema.edgeTypeColumnName} to be imported as Graph Frame")

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

  /**
   * validates the imported vertex schema is proper to represent a vertices frame schema
   *
   * @param vertexSchema vertex schema
   */
  def validateVertexSchema(vertexSchema: StructType) = {
    require(vertexSchema.fieldNames.contains(GraphSchema.vertexIdColumnName), s"Schema must contain field named ${GraphSchema.vertexIdColumnName}")
  }

  /**
   * validates the imported edge schema is proper to represent a edges frame schema
   *
   * @param edgeSchema edge schema
   */
  def validateEdgeSchema(edgeSchema: StructType) = {
    require(edgeSchema.fieldNames.contains(GraphSchema.edgeSourceColumnName), s"Schema must contain field named $GraphSchema.edgeSourceColumnName")
    require(edgeSchema.fieldNames.contains(GraphSchema.edgeDestinationColumnName), s"Schema must contain field named $GraphSchema.edgeDestinationColumnName")
  }
}

/**
 * column names are used by OrientDB graph
 */
object graphParameters {

  //the exported graph frame ID
  val orientVertexId = GraphFrame.ID + "_"
}
