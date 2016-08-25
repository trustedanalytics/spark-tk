package org.trustedanalytics.sparktk.graph.internal.constructors.fromorientdb

import com.tinkerpop.blueprints.Vertex
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.graphframes.GraphFrame

class VertexReader(graph: OrientGraphNoTx, vertexSchema: StructType) {

  /**
   * converts OrientDB vertex to Apache Spark SQL Row
   *
   * @param orientVertex OrientDB vertex
   * @return Apache Spark SQL Row
   */
  def importVertex(orientVertex: Vertex): Row = {
    try {
      createVertex(orientVertex)
    }
    catch {
      case e: Exception =>
        throw new RuntimeException(s"Unable to read vertex with ID ${orientVertex.getId.toString} from OrientDB graph: ${e.getMessage}")
    }
  }

  /**
   * A method creates Spark SQL row
   *
   * @param orientVertex OrientDB vertex
   * @return Apache Spark SQL Row
   */
  def createVertex(orientVertex: Vertex): Row = {
    val row = vertexSchema.fields.map(field => {
      if (field.name == GraphFrame.ID) {
        orientVertex.getProperty(graphParameters.orientVertexId): Any
      }
      else {
        orientVertex.getProperty(field.name): Any
      }
    })
    Row.fromSeq(row.toSeq)
  }

}
