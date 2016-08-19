package org.trustedanalytics.sparktk.graph.internal.ops.orientdb

import org.graphframes.GraphFrame
import org.trustedanalytics.sparktk.graph.internal.{ GraphState, GraphSchema }

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
  def saveToOrientGraph(batchSize: Int, dbUrl: String, userName: String, password: String, rootPassword: String) = {
    val orientConf = OrientConf(dbUrl, userName, password, rootPassword)
    val orientGraph = OrientdbGraphFactory.graphDbConnector(orientConf)
    val schemaWriter = new SchemaWriter(orientGraph)
    if (orientGraph.getVertexType(GraphSchema.vertexTypeColumnName) == null) {
      schemaWriter.vertexSchema(state.graphFrame.vertices.schema, GraphSchema.vertexTypeColumnName)
    }
    if (orientGraph.getEdgeType(GraphFrame.EDGE) == null) {
      schemaWriter.edgeSchema(state.graphFrame.edges.schema)
    }
    orientGraph.shutdown(true, true)
    val vertexFrameWriter = new VertexFrameWriter(state.graphFrame.vertices, orientConf)
    vertexFrameWriter.exportVertexFrame(batchSize)
    val edgeFrameWriter = new EdgeFrameWriter(state.graphFrame.edges, orientConf)
    edgeFrameWriter.exportEdgeFrame(batchSize)
  }
}
