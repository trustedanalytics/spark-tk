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

import com.tinkerpop.blueprints.{ Vertex, Parameter }
import com.tinkerpop.blueprints.impls.orient.{ OrientEdgeType, OrientVertexType, OrientGraphNoTx }
import org.apache.spark.sql.types.StructType
import org.graphframes.GraphFrame
import org.trustedanalytics.sparktk.graph.internal.GraphSchema

/**
 * exports the graph frame schema to OrientDB graph schema
 *
 * @param orientGraph OrientDB graph database
 */
class SchemaWriter(orientGraph: OrientGraphNoTx) {

  /**
   * exports vertex schema to OrientDB vertex schema
   *
   * @param vertexSchema vertex schema
   * @param vertexType vertex type
   */
  def vertexSchema(vertexSchema: StructType, vertexType: String): OrientVertexType = {

    try {
      val orientVertexType = orientGraph.createVertexType(vertexType)
      val vertexSchemaIterator = vertexSchema.iterator
      while (vertexSchemaIterator.hasNext) {
        val columnField = vertexSchemaIterator.next()
        val orientColumnDataType = DataTypesConverter.sparkToOrientdb(columnField.dataType)
        if (columnField.name == GraphFrame.ID) {
          orientVertexType.createProperty(exportGraphParam.vertexId, orientColumnDataType)
        }
        else {
          orientVertexType.createProperty(columnField.name, orientColumnDataType)
        }
      }
      orientGraph.createKeyIndex(exportGraphParam.vertexId, classOf[Vertex], new Parameter("type", "UNIQUE"), new Parameter("class", vertexType))
      orientVertexType
    }
    catch {
      case e: Exception =>
        orientGraph.rollback()
        throw new RuntimeException(s"Unable to create the vertex schema:${e.getMessage}")
    }
  }

  /**
   * exports edge schema to OrientDB edge schema
   *
   * @param edgeSchema edge schema
   */
  def edgeSchema(edgeSchema: StructType): OrientEdgeType = {
    try {
      val orientEdgeType = orientGraph.createEdgeType(GraphSchema.edgeTypeColumnName)
      edgeSchema.fields.map(col => {
        val orientColumnDataType = DataTypesConverter.sparkToOrientdb(col.dataType)
        if (col.name == GraphFrame.EDGE) {
          orientEdgeType.createProperty(GraphSchema.edgeTypeColumnName, orientColumnDataType)
        }
        else {
          orientEdgeType.createProperty(col.name, orientColumnDataType)
        }
      })
      orientEdgeType
    }
    catch {
      case e: Exception =>
        orientGraph.rollback()
        throw new RuntimeException(s"Unable to create the edge schema: ${e.getMessage}")
    }
  }

}

/**
 * hard coded parameters
 */
object exportGraphParam {
  val vertexId = GraphFrame.ID + "_"
}
