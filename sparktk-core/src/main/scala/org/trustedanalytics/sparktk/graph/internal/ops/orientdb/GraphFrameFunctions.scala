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
   * @param batchSize batch size
   * @param dbUrl OrientDB database full URI
   * @param userName the database user name
   * @param password the database password
   * @param rootPassword OrientDB server password
   * @return
   */
  def saveToOrientGraph(batchSize: Int, dbUrl: String, userName: String, password: String, rootPassword: String): ExportToOrientdbReturn = {
    val exportedVertices = mutable.Map[String, Statistics]()
    val exportedEdges = mutable.Map[String, Statistics]()
    val orientConf = OrientConf(dbUrl, userName, password, rootPassword, batchSize)
    val orientGraph = OrientdbGraphFactory.graphDbConnector(orientConf)
    val schemaWriter = new SchemaWriter(orientGraph)
    if (orientGraph.getVertexType(GraphSchema.vertexTypeColumnName) == null) {
      schemaWriter.vertexSchema(state.graphFrame.vertices.schema, GraphSchema.vertexTypeColumnName)
    }
    if (orientGraph.getEdgeType(GraphSchema.edgeTypeColumnName) == null) {
      schemaWriter.edgeSchema(state.graphFrame.edges.schema)
    }
    orientGraph.shutdown(true, true)
    val vertexFrameWriter = new VertexFrameWriter(state.graphFrame.vertices, orientConf)
    val verticesCount = vertexFrameWriter.exportVertexFrame(orientConf.batchSize)
    val edgeFrameWriter = new EdgeFrameWriter(state.graphFrame.edges, orientConf)
    val edgesCount = edgeFrameWriter.exportEdgeFrame(orientConf.batchSize)

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
    val exportedVerticesCount = orientGraph.countVertices(GraphSchema.vertexTypeColumnName)
    val verticesFailureCount = verticesCount - exportedVerticesCount
    exportedVertices.put(GraphSchema.vertexTypeColumnName, Statistics(exportedVerticesCount, verticesFailureCount))
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
    val exportedEdgesCount = orientGraph.countEdges(GraphSchema.edgeTypeColumnName)
    val edgesFailureCount = edgesCount - exportedEdgesCount
    exportedEdges.put(GraphSchema.edgeTypeColumnName, Statistics(exportedEdgesCount, edgesFailureCount))
    exportedEdges.toMap
  }
}
