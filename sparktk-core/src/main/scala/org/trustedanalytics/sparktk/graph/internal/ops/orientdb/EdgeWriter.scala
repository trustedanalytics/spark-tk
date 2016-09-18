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
    val edgeType = if (edgeTypeColumnName.isDefined) { row.getAs[String](edgeTypeColumnName.get) } else { orientGraph.getEdgeBaseType.getName }
    val orientEdge = orientGraph.addEdge("class:" + edgeType, srcVertex, dstVertex, null)
    val propKeysIterator = orientGraph.getRawGraph.getMetadata.getSchema.getClass(edgeType).properties().iterator()
    while (propKeysIterator.hasNext) {
      val propKey = propKeysIterator.next().getName
      if (row.getAs(propKey) != null) orientEdge.setProperty(propKey, row.getAs(propKey))
    }
    orientEdge
  }

  /**
   * finds OrientDB edge
   *
   * @param row row
   * @return OrientDB edge
   */
  def find(row: Row): Option[Edge] = {
    val edges = orientGraph.getEdgesOfClass(orientGraph.getEdgeBaseType.getName, GraphFrame.SRC == row.getAs(GraphFrame.SRC) && GraphFrame.DST == row.getAs(GraphFrame.DST))
    val edgeIterator = edges.iterator()
    if (edgeIterator.hasNext) {
      val existingEdge = edgeIterator.next()
      return Some(existingEdge)
    }
    None
  }

  /**
   * updates OrientDB edge
   *
   * @param row row
   * @param orientEdge OrientDB edge
   * @return updated OrientDB edge
   */
  def update(row: Row, orientEdge: Edge): Edge = {
    val rowSchemaIterator = row.schema.iterator
    while (rowSchemaIterator.hasNext) {
      val propName = rowSchemaIterator.next().name
      orientEdge.setProperty(propName, row.getAs(propName))
    }
    orientEdge
  }

  /**
   * updates OrientDB edge if exists or creates a new edge if not found
   *
   * @param row row
   * @return OrientDB edge
   */
  def updateOrCreate(row: Row): Edge = {
    val orientEdge = find(row)
    val newEdge = if (orientEdge.isEmpty) {
      create(row)
    }
    else {
      update(row, orientEdge.get)
    }
    newEdge
  }

}
