package org.trustedanalytics.sparktk.graph.internal.ops.orientdb

import org.trustedanalytics.sparktk.graph.internal.{ GraphState, GraphSummarization, BaseGraph }

trait ExportToOrientdbSummarization extends BaseGraph {

  /**
   * Save GraphFrame to OrientDB graph
   *
   * @param batchSize batch size
   * @param dbUrl OrientDB database full URI
   * @param userName the database user name
   * @param password the database password
   * @param rootPassword OrientDB server password
   * @param vertexTypeColumnName vertex type column name
   * @param edgeTypeColumnName edge type column name
   * @return summary statistics for the number of exported edges and vertices
   */
  def exportToOrientdb(batchSize: Int, dbUrl: String, userName: String, password: String, rootPassword: String, vertexTypeColumnName: Option[String] = None, edgeTypeColumnName: Option[String] = None): ExportToOrientdbReturn = {
    execute(ExportToOrientdb(batchSize, dbUrl, userName, password, rootPassword, vertexTypeColumnName, edgeTypeColumnName))
  }
}

case class ExportToOrientdb(batchSize: Int, dbUrl: String, userName: String, password: String, rootPassword: String, vertexTypeColumnName: Option[String] = None, edgeTypeColumnName: Option[String] = None) extends GraphSummarization[ExportToOrientdbReturn] {

  override def work(state: GraphState): ExportToOrientdbReturn = {

    val exporter = new GraphFrameFunctions(state)
    exporter.saveToOrientGraph(batchSize, dbUrl, userName, password, rootPassword, vertexTypeColumnName, edgeTypeColumnName)
  }
}

import scala.collection.immutable.Map

/**
 * returns the output arguments of ExportOrientDbGraphPlugin
 * @param exportedVertices a dictionary of vertex classname and the corresponding statistics of exported vertices
 * @param exportedEdges a dictionary of edge classname and the corresponding statistics of exported edges.
 * @param dbUri the database URI
 */

case class ExportToOrientdbReturn(exportedVertices: Map[String, Statistics], exportedEdges: Map[String, Statistics], dbUri: String)

/**
 * returns statistics for the exported graph elements
 * @param exportedCount the number of the exported elements
 * @param failureCount the number of elements failed to be exported.
 */
case class Statistics(exportedCount: Long, failureCount: Long)
