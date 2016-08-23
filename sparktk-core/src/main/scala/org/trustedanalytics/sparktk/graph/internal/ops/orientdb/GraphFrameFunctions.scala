package org.trustedanalytics.sparktk.graph.internal.ops.orientdb

import org.trustedanalytics.sparktk.graph.internal.{ GraphState, GraphSchema }
import collection.JavaConverters._
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
    val orientConf = OrientConf(dbUrl, userName, password, rootPassword)
    val orientGraph = OrientdbGraphFactory.graphDbConnector(orientConf)
    val schemaWriter = new SchemaWriter(orientGraph)
    if (orientGraph.getVertexType(GraphSchema.vertexTypeColumnName) == null) {
      schemaWriter.vertexSchema(state.graphFrame.vertices.schema, GraphSchema.vertexTypeColumnName)
    }
    if (orientGraph.getEdgeType(schemaWriter.exportedEdgeType) == null) {
      schemaWriter.edgeSchema(state.graphFrame.edges.schema)
    }
    orientGraph.shutdown(true, true)
    val vertexFrameWriter = new VertexFrameWriter(state.graphFrame.vertices, orientConf)
    val verticesCount = vertexFrameWriter.exportVertexFrame(batchSize)
    val edgeFrameWriter = new EdgeFrameWriter(state.graphFrame.edges, orientConf)
    val edgesCount = edgeFrameWriter.exportEdgeFrame(batchSize)

    //collect statistics
    val instanceOfOrientGraph = OrientdbGraphFactory.graphDbConnector(orientConf)
    val exportedVerticesCount = instanceOfOrientGraph.countVertices(GraphSchema.vertexTypeColumnName)
    val verticesFailureCount = verticesCount - exportedVerticesCount
    exportedVertices.put(GraphSchema.vertexTypeColumnName, Statistics(exportedVerticesCount, verticesFailureCount))

    val exportedEdgesCount = instanceOfOrientGraph.countEdges(schemaWriter.exportedEdgeType)
    val edgesFailureCount = edgesCount - exportedEdgesCount
    exportedEdges.put(schemaWriter.exportedEdgeType, Statistics(exportedEdgesCount, edgesFailureCount))
    instanceOfOrientGraph.shutdown()
    new ExportToOrientdbReturn(exportedVertices.toMap, exportedEdges.toMap, orientConf.dbUri)
  }
}
