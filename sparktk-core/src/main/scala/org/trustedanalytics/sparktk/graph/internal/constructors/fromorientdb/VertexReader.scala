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

import com.tinkerpop.blueprints.Vertex
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.graphframes.GraphFrame

class VertexReader(graph: OrientGraphNoTx, vertexSchema: StructType) {

  /**
   * converts OrientDB vertex to graph frame vertex
   *
   * @param orientVertex OrientDB vertex
   * @return graph frame vertex
   */
  def importVertex(orientVertex: Vertex): Row = {
    try {
      createVertex(orientVertex)
    }
    catch {
      case e: Exception =>
        throw new RuntimeException(s"Unable to read vertex with ID ${orientVertex.getId.toString} from OrientDB graph: ${e.getMessage}")
    }
  }

  /**
   * creates graph frame vertex
   *
   * @param orientVertex OrientDB vertex
   * @return graph frame vertex
   */
  def createVertex(orientVertex: Vertex): Row = {
    val row = vertexSchema.fields.map(field => {
      if (field.name == GraphFrame.ID) {
        orientVertex.getProperty(graphParameters.orientVertexId): Any
      }
      else {
        orientVertex.getProperty(field.name): Any
      }
    })
    Row.fromSeq(row.toSeq)
  }

}
