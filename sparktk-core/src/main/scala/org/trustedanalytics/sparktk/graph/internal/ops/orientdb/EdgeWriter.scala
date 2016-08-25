package org.trustedanalytics.sparktk.graph.internal.ops.orientdb

import com.tinkerpop.blueprints.Edge
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx
import org.apache.spark.sql.Row
import org.graphframes.GraphFrame
import org.trustedanalytics.sparktk.graph.internal.GraphSchema

/**
 * exports Spark SQL Row to OrientDB edge
 *
 * @param orientGraph OrientDB graph database
 */
class EdgeWriter(orientGraph: OrientGraphNoTx) {

  /**
   * exports row to OrientDB vertex
   *
   * @param row edge row
   * @param vertexType vertices type or class name
   */
  def create(row: Row, vertexType: String): Edge = {
    //lookup the source and destination vertices for this row
    val vertexWriter = new VertexWriter(orientGraph)
    val srcVertex = vertexWriter.findOrCreate(row.getAs(GraphFrame.SRC), vertexType)
    val dstVertex = vertexWriter.findOrCreate(row.getAs(GraphFrame.DST), vertexType)
    //create OrientDB edge
    val orientEdge = orientGraph.addEdge("class:" + GraphSchema.edgeTypeColumnName, srcVertex, dstVertex, null)
    val rowSchemaIterator = row.schema.iterator
    while (rowSchemaIterator.hasNext) {
      val propName = rowSchemaIterator.next().name
      orientEdge.setProperty(propName, row.getAs(propName))
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
    val edges = orientGraph.getEdgesOfClass(GraphSchema.edgeTypeColumnName, GraphFrame.SRC == row.getAs(GraphFrame.SRC) && GraphFrame.DST == row.getAs(GraphFrame.DST))
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
   * @param vertexType vertex type or class name
   * @return OrientDB edge
   */
  def updateOrCreate(row: Row, vertexType: String): Edge = {
    val orientEdge = find(row)
    val newEdge = if (orientEdge.isEmpty) {
      create(row, vertexType)
    }
    else {
      update(row, orientEdge.get)
    }
    newEdge
  }

}
