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

import org.trustedanalytics.sparktk.graph.internal.constructors.fromorientdb.SchemaReader
import scala.collection.immutable.Map
import scala.collection.mutable

/**
 * collect Statistics
 *
 * @param orientConf    OrientDB configurations
 * @return exported vertices and edges summary statistics
 */
class Statistics(orientConf: OrientdbConf, dbName: String) {
  val orientGraphInstance = OrientdbGraphFactory.graphDbConnector(orientConf, dbName)

  /**
   * collect Statistics
   *
   * @param verticesCount count of vertices required to be exported to OrientDB database
   * @param edgesCount    count of edges required to be exported to OrientDB database
   * @return exported vertices and edges summary statistics
   */
  def getStats(verticesCount: Long, edgesCount: Long): ExportToOrientdbReturn = {
    val dbUri = OrientdbGraphFactory.getUrl(orientConf, dbName)
    val verticesSummary = getExportedVerticesSummary(verticesCount)
    val verticesTypesStats = getExportVertexClassStats
    val edgesSummary = getExportedEdgesSummary(edgesCount)
    val edgesTypesStats = getExportEdgeClassStats
    orientGraphInstance.shutdown()
    new ExportToOrientdbReturn(verticesSummary, verticesTypesStats, edgesSummary, edgesTypesStats, dbUri)
  }

  /**
   * Get vertices types statistics
   *
   * @return dictionary for the exported vertices types and count
   */
  def getExportVertexClassStats: Map[String, Long] = {
    val exportedVertices = mutable.Map[String, Long]()
    val schemaReader = new SchemaReader(orientGraphInstance)
    val vertexTypes = schemaReader.getVertexClasses.getOrElse(Set(orientGraphInstance.getVertexBaseType.getName))
    vertexTypes.foreach(vertexType => {
      exportedVertices.put(vertexType, orientGraphInstance.countVertices(vertexType))
    })
    exportedVertices.toMap
  }

  /**
   * Get edges types statistics
   *
   * @return dictionary for the exported edges types and count
   */
  def getExportEdgeClassStats: Map[String, Long] = {
    val exportedEdges = mutable.Map[String, Long]()
    val schemaReader = new SchemaReader(orientGraphInstance)
    val edgeTypes = schemaReader.getEdgeClasses.getOrElse(Set(orientGraphInstance.getEdgeBaseType.getName))
    edgeTypes.foreach(edgeType => {
      val count = orientGraphInstance.countVertices(edgeType)
      exportedEdges.put(edgeType, orientGraphInstance.countVertices(edgeType))
    })
    exportedEdges.toMap
  }

  /**
   * Get the exported vertices summary statistics
   *
   * @param verticesCount count of vertices required to be exported to OrientDB database
   * @return dictionary for the exported vertices success and failure count
   */
  def getExportedVerticesSummary(verticesCount: Long): Map[String, Long] = {
    val successCountLabel = "Total Exported Vertices Count"
    val failureCountLabel = "Failure Count"
    val stats = mutable.Map[String, Long]()
    stats.put(successCountLabel, orientGraphInstance.countVertices())
    stats.put(failureCountLabel, verticesCount - orientGraphInstance.countVertices())
    stats.toMap
  }

  /**
   * Get the exported edges summary statistics
   *
   * @param edgesCount count of edges required to be exported to OrientDB database
   * @return dictionary for the exported edges success and failure count
   */
  def getExportedEdgesSummary(edgesCount: Long): Map[String, Long] = {
    val successCountLabel = "Total Exported Edges Count"
    val failureCountLabel = "Failure Count"
    val stats = mutable.Map[String, Long]()
    stats.put(successCountLabel, orientGraphInstance.countEdges())
    stats.put(failureCountLabel, edgesCount - orientGraphInstance.countEdges())
    stats.toMap
  }
}
