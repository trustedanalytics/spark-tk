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

import com.tinkerpop.blueprints.Vertex
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx
import org.apache.spark.sql.Row
import org.graphframes.GraphFrame
import collection.JavaConverters._
import scala.collection.mutable

/**
 * exports graph frame vertex to OrientDB
 *
 * @param orientGraph OrientDB graph database
 */
class VertexWriter(orientGraph: OrientGraphNoTx) {

  /**
   * converts Spark SQL Row to OrientDB vertex
   *
   * @param vertexClassName vertex type or class name
   * @param row             row
   */
  def create(vertexClassName: String, row: Row): Vertex = {
    val propMap = mutable.Map[String, Any]()
    val propKeysIterator = orientGraph.getRawGraph.getMetadata.getSchema.getClass(vertexClassName).properties().iterator()
    while (propKeysIterator.hasNext) {
      val propKey = propKeysIterator.next().getName
      if (propKey == exportGraphParam.vertexId) {
        propMap.put(propKey, row.getAs(GraphFrame.ID))
      }
      else {
        propMap.put(propKey, row.getAs(propKey))
      }
    }
    orientGraph.addVertex(s"class:$vertexClassName", propMap.asJava)
  }

  /**
   * finds a vertex
   *
   * @param vertexId vertex ID
   * @return OrientDB vertex if exists or null if not found
   */
  def find(vertexId: Any, className: String): Option[Vertex] = {
    val vertices = orientGraph.getVertices(exportGraphParam.vertexId, vertexId)
    val vertexIterator = vertices.iterator()
    if (vertexIterator.hasNext) {
      val existingVertex = vertexIterator.next()
      return Some(existingVertex)
    }
    None
  }

  /**
   * looking up a vertex in OrientDB graph or creates a new vertex if not found
   *
   * @param vertexId  vertex ID
   * @param className vertex type or class name
   * @return OrientDB vertex
   */
  def findOrCreate(vertexId: Any, className: String): Vertex = {
    val vertex = find(vertexId, className)
    if (vertex.isEmpty) {
      orientGraph.addVertex(s"class:$className", exportGraphParam.vertexId, vertexId.toString)
    }
    else {
      vertex.get
    }
  }

}
