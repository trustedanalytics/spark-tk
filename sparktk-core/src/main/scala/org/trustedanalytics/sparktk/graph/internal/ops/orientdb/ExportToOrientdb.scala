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
