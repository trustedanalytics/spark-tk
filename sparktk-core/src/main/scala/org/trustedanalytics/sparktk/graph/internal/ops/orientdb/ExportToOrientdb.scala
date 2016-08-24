package org.trustedanalytics.sparktk.graph.internal.ops.orientdb

import org.trustedanalytics.sparktk.graph.internal.{ GraphState, GraphSummarization, BaseGraph }

trait ExportToOrientdbSummarization extends BaseGraph {

  /**
   * Save the current frame as OrientDB graph.
   *
   * @param batchSize
   * @param dbUrl
   * @param userName
   * @param password
   * @param rootPassword
   */
  def exportToOrientdb(batchSize: Int, dbUrl: String, userName: String, password: String, rootPassword: String): ExportToOrientdbReturn = {
    execute(ExportToOrientdb(batchSize, dbUrl, userName, password, rootPassword))
  }
}

case class ExportToOrientdb(batchSize: Int, dbUrl: String, userName: String, password: String, rootPassword: String) extends GraphSummarization[ExportToOrientdbReturn] {

  override def work(state: GraphState): ExportToOrientdbReturn = {

    val exporter = new GraphFrameFunctions(state)
    exporter.saveToOrientGraph(batchSize, dbUrl, userName, password, rootPassword)
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
