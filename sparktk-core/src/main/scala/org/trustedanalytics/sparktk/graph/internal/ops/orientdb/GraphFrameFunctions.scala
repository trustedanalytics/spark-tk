package org.trustedanalytics.sparktk.graph.internal.ops.orientdb

import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx
import org.trustedanalytics.sparktk.graph.internal.{ GraphState, GraphSchema }
import scala.collection.immutable.Map
import scala.collection.mutable

/**
 * exports a graph frame to OrientDB graph
 *
 * @param state graph frame
 */
class GraphFrameFunctions(state: GraphState) {

  /**
   * save GraphFrame to OrientDB graph
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
  def saveToOrientGraph(batchSize: Int, dbUrl: String, userName: String, password: String, rootPassword: String, vertexTypeColumnName: Option[String] = None, edgeTypeColumnName: Option[String] = None): ExportToOrientdbReturn = {
    val exportedVertices = mutable.Map[String, Statistics]()
    val exportedEdges = mutable.Map[String, Statistics]()
    val orientConf = OrientConf(dbUrl, userName, password, rootPassword, batchSize)
    val orientGraph = OrientdbGraphFactory.graphDbConnector(orientConf)
    val schemaWriter = new SchemaWriter
    schemaWriter.vertexSchema(state.graphFrame.vertices, orientGraph, vertexTypeColumnName)
    schemaWriter.edgeSchema(state.graphFrame.edges, orientGraph, edgeTypeColumnName)
    orientGraph.shutdown(true, true)
    val vertexFrameWriter = new VertexFrameWriter(state.graphFrame.vertices, orientConf)
    val verticesCount = vertexFrameWriter.exportVertexFrame(orientConf.batchSize, vertexTypeColumnName)
    val edgeFrameWriter = new EdgeFrameWriter(state.graphFrame.edges, orientConf)
    val edgesCount = edgeFrameWriter.exportEdgeFrame(orientConf.batchSize, edgeTypeColumnName)

    //collect statistics
    val orientGraphInstance = OrientdbGraphFactory.graphDbConnector(orientConf)
    val verticesStats = getExportVertexStats(verticesCount, orientGraphInstance)
    val edgesStats = getExportEdgeStats(edgesCount, orientGraphInstance)
    orientGraphInstance.shutdown()
    new ExportToOrientdbReturn(verticesStats, edgesStats, orientConf.dbUri)
  }

  /**
   * collects the exported vertices statistics
   * @param verticesCount the number of vertices to be exported
   * @param orientGraph an instance of OrientDB graph
   * @return dictionary for the exported vertices statistics
   */
  def getExportVertexStats(verticesCount: Long, orientGraph: OrientGraphNoTx): Map[String, Statistics] = {
    val exportedVertices = mutable.Map[String, Statistics]()
    val exportedVerticesCount = orientGraph.countVertices()
    val verticesFailureCount = verticesCount - exportedVerticesCount
    exportedVertices.put("vertices", Statistics(exportedVerticesCount, verticesFailureCount))
    exportedVertices.toMap
  }

  /**
   * collects the exported edges statistics
   * @param edgesCount the number of edges to be exported
   * @param orientGraph an instance of OrientDB graph
   * @return dictionary for the exported edges statistics
   */
  def getExportEdgeStats(edgesCount: Long, orientGraph: OrientGraphNoTx): Map[String, Statistics] = {
    val exportedEdges = mutable.Map[String, Statistics]()
    val exportedEdgesCount = orientGraph.countEdges()
    val edgesFailureCount = edgesCount - exportedEdgesCount
    exportedEdges.put("edges", Statistics(exportedEdgesCount, edgesFailureCount))
    exportedEdges.toMap
  }
}
