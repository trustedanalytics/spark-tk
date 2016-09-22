package org.trustedanalytics.sparktk.graph.internal.ops.orientdb

import com.tinkerpop.blueprints.Edge
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx
import org.apache.spark.sql.Row
import org.graphframes.GraphFrame
import org.trustedanalytics.sparktk.graph.internal.GraphSchema

/**
 * exports Spark SQL Row to OrientDB edge
 * @param orientGraph OrientDB graph database
 */
class EdgeWriter(orientGraph: OrientGraphNoTx) {

  /**
   * exports row to OrientDB vertex
   * @param row edge row
   *  @param edgeTypeColumnName the given column name for edge type
   */
  def create(row: Row, edgeTypeColumnName: Option[String] = None): Edge = {
    //lookup the source and destination vertices for this row
    val vertexWriter = new VertexWriter(orientGraph)
    val srcVertex = vertexWriter.findOrCreate(row.getAs(GraphFrame.SRC))
    val dstVertex = vertexWriter.findOrCreate(row.getAs(GraphFrame.DST))
    //create OrientDB edge
    val edgeType = if (edgeTypeColumnName.isDefined) {
      row.getAs[String](edgeTypeColumnName.get)
    }
    else {
      orientGraph.getEdgeBaseType.getName
    }
    val orientEdge = orientGraph.addEdge("class:" + edgeType, srcVertex, dstVertex, null)
    val propKeysIterator = orientGraph.getRawGraph.getMetadata.getSchema.getClass(edgeType).properties().iterator()
    while (propKeysIterator.hasNext) {
      val propKey = propKeysIterator.next().getName
      if (row.getAs(propKey) != null) orientEdge.setProperty(propKey, row.getAs(propKey))
    }
    orientEdge
  }

}
