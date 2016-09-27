package org.trustedanalytics.sparktk.graph.internal.ops.orientdb

import org.trustedanalytics.sparktk.graph.internal.{ GraphState, GraphSummarization, BaseGraph }

trait ExportToOrientdbSummarization extends BaseGraph {

  /**
   * Save GraphFrame to OrientDB graph
   *
   * @param dbUrl OrientDB database full URI
   * @param userName the database user name
   * @param password the database password
   * @param rootPassword OrientDB server password
   * @param vertexTypeColumnName vertex type column name
   * @param edgeTypeColumnName edge type column name
   * @param batchSize batch size
   * @return summary statistics for the number of exported edges and vertices
   */
  def exportToOrientdb(dbUrl: String, userName: String, password: String, rootPassword: String, vertexTypeColumnName: Option[String] = None, edgeTypeColumnName: Option[String] = None, batchSize: Int = 1000): ExportToOrientdbReturn = {
    execute(ExportToOrientdb(dbUrl, userName, password, rootPassword, vertexTypeColumnName, edgeTypeColumnName, batchSize))
  }
}

case class ExportToOrientdb(dbUrl: String, userName: String, password: String, rootPassword: String, vertexTypeColumnName: Option[String] = None, edgeTypeColumnName: Option[String] = None, batchSize: Int = 1000) extends GraphSummarization[ExportToOrientdbReturn] {

  override def work(state: GraphState): ExportToOrientdbReturn = {

    val graphFrameExporter = new GraphFrameFunctions(state)
    graphFrameExporter.saveToOrientGraph(dbUrl, userName, password, rootPassword, vertexTypeColumnName, edgeTypeColumnName, batchSize)
  }
}

/**
 * returns the output arguments of ExportOrientDbGraphPlugin
 * @param exportedVerticesSummary a dictionary of the total number of exported vertices and the failure count
 * @param verticesTypes a dictionary of vertex class names and the corresponding statistics of exported vertices
 * @param exportedEdgesSummary a dictionary of the total number of exported edges and the failure count
 * @param edgesTypes a dictionary of edge class names and the corresponding statistics of exported edges.
 * @param dbUri the database URI
 */
case class ExportToOrientdbReturn(exportedVerticesSummary: Map[String, Long], verticesTypes: Map[String, Long], exportedEdgesSummary: Map[String, Long], edgesTypes: Map[String, Long], dbUri: String)
