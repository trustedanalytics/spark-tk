package org.trustedanalytics.sparktk.graph.internal.ops.orientdb

import com.tinkerpop.blueprints.Vertex
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx
import org.apache.spark.sql.Row
import org.graphframes.GraphFrame

/**
 * exports Spark graph frame vertex to OrientDB
 *
 * @param orientGraph OrientDB graph database
 */
class VertexWriter(orientGraph: OrientGraphNoTx) {

  /**
   * reads Spark graphframe vertex and converts it to a new OrientDB vertex
   *
   * @param vertexClassName vertex type or class name
   * @param row Spark graph frame vertex
   */
  def create(vertexClassName: String, row: Row): Vertex = {
    val orientVertex: Vertex = orientGraph.addVertex(vertexClassName, null)
    val rowSchemaIterator = row.schema.iterator
    while (rowSchemaIterator.hasNext) {
      val propName = rowSchemaIterator.next().name
      if (propName == GraphFrame.ID) {
        orientVertex.setProperty(GraphFrame.ID + "_", row.getAs(propName))
      }
      else {
        orientVertex.setProperty(propName, row.getAs(propName))
      }
    }
    orientVertex
  }

  /**
   * a method that finds a vertex
   *
   * @param vertexId vertex ID
   * @return  OrientDB vertex if exists or null if not found
   */
  def find(vertexId: Any, className: String): Option[Vertex] = {
    val vertices = orientGraph.getVertices(GraphFrame.ID + "_", vertexId)
    val vertexIterator = vertices.iterator()
    if (vertexIterator.hasNext) {
      val existingVertex = vertexIterator.next()
      return Some(existingVertex)
    }
    None
  }

  /**
   * a method for looking up a vertex in OrientDB graph or creates a new vertex if not found
   *
   * @param vertexId vertex ID
   * @param className vertex type or class name
   * @return OrientDB vertex
   */
  def findOrCreate(vertexId: Any, className: String): Vertex = {
    val vertex = find(vertexId, className)
    if (vertex.isEmpty) {
      val newVertex = orientGraph.addVertex(className, null)
      newVertex.setProperty(GraphFrame.ID + "_", vertexId)
      newVertex
    }
    else {
      vertex.get
    }
  }

  /**
   * updates an existing OrientDB vertex
   *
   * @param row row
   * @param orientVertex OrientDB vertex
   * @return updated OrientDB vertex
   */
  def update(row: Row, orientVertex: Vertex): Vertex = {
    val rowSchemaIterator = row.schema.iterator
    while (rowSchemaIterator.hasNext) {
      val propName = rowSchemaIterator.next().name
      if (propName == GraphFrame.ID) {
        orientVertex.setProperty(GraphFrame.ID + "_", row.getAs(propName))
      }
      else {
        orientVertex.setProperty(propName, row.getAs(propName))
      }
    }
    orientVertex
  }

  /**
   *
   * @param row row
   * @param className vertex type or class name
   * @return OrientDB vertex
   */
  def updateOrCreate(row: Row, className: String): Vertex = {
    val orientVertex = find(row.getAs(GraphFrame.ID), className)
    val newVertex = if (orientVertex.isEmpty) {
      create(className, row)
    }
    else {
      update(row, orientVertex.get)
    }
    newVertex
  }

}
