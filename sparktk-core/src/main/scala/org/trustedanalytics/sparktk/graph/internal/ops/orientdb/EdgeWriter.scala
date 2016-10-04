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

import com.tinkerpop.blueprints.Edge
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx
import org.apache.spark.sql.Row
import org.graphframes.GraphFrame
import org.trustedanalytics.sparktk.graph.internal.GraphSchema

/**
 * exports Spark SQL Row to OrientDB edge
 * @param orientGraph OrientDB graph database
 */
class EdgeWriter(orientGraph: OrientGraphNoTx) {

  /**
   * exports row to OrientDB vertex
   * @param row edge row
   *  @param edgeTypeColumnName the given column name for edge type
   */
  def create(row: Row, edgeTypeColumnName: Option[String] = None): Edge = {
    //lookup the source and destination vertices for this row
    val vertexWriter = new VertexWriter(orientGraph)
    val srcVertex = vertexWriter.findOrCreate(row.getAs(GraphFrame.SRC))
    val dstVertex = vertexWriter.findOrCreate(row.getAs(GraphFrame.DST))
    //create OrientDB edge
    val edgeType = if (edgeTypeColumnName.isDefined) {
      row.getAs[String](edgeTypeColumnName.get)
    }
    else {
      orientGraph.getEdgeBaseType.getName
    }
    val orientEdge = orientGraph.addEdge("class:" + edgeType, srcVertex, dstVertex, null)
    val propKeysIterator = orientGraph.getRawGraph.getMetadata.getSchema.getClass(edgeType).properties().iterator()
    while (propKeysIterator.hasNext) {
      val propKey = propKeysIterator.next().getName
      if (row.getAs(propKey) != null) orientEdge.setProperty(propKey, row.getAs(propKey))
    }
    orientEdge
  }

}
