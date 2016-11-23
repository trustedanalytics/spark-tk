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

import org.trustedanalytics.sparktk.graph.internal.GraphState

/**
 * Export graph to OrientDB graph
 */
class GraphFrameFunctions(state: GraphState) {

  /**
   *  Save GraphFrame to OrientDB graph
   * @param orientConf the database configurations
   * @param dbName the database name
   * @param vertexTypeColumnName vertex type column name
   * @param edgeTypeColumnName edge type column name
   * @return summary statistics for the number of exported edges and vertices
   */
  def saveToOrientGraph(orientConf: OrientdbConf,
                        dbName: String,
                        vertexTypeColumnName: Option[String] = None,
                        edgeTypeColumnName: Option[String] = None,
                        batchSize: Int = 1000,
                        dbProperties: Option[Map[String, Any]] = None): ExportOrientdbStats = {

    val orientGraph = OrientdbGraphFactory.graphDbConnector(orientConf, dbName, dbProperties)
    //export schema
    val schemaWriter = new SchemaWriter
    schemaWriter.vertexSchema(state.graphFrame.vertices, orientGraph, vertexTypeColumnName)
    schemaWriter.edgeSchema(state.graphFrame.edges, orientGraph, edgeTypeColumnName)
    orientGraph.shutdown(true, true)
    //export graph
    val vertexFrameWriter = new VertexFrameWriter(state.graphFrame.vertices, orientConf, dbName)
    val verticesCount = vertexFrameWriter.exportVertexFrame(batchSize, vertexTypeColumnName)
    val edgeFrameWriter = new EdgeFrameWriter(state.graphFrame.edges, orientConf, dbName)
    val edgesCount = edgeFrameWriter.exportEdgeFrame(batchSize, edgeTypeColumnName)
    //collect statistics
    val stats = new Statistics(orientConf, dbName)
    stats.getStats(verticesCount, edgesCount)
  }
}
