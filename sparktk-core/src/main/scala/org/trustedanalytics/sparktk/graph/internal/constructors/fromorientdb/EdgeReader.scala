package org.trustedanalytics.sparktk.graph.internal.constructors.fromorientdb

import com.tinkerpop.blueprints.Edge
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

/**
 * converts OrientDB edge to Spark SQL Row
 *
 * @param graph      OrientDB graph
 * @param edgeSchema edges schema
 */
class EdgeReader(graph: OrientGraphNoTx, edgeSchema: StructType) {

  /**
   * A method imports OrientDB edge from OrientDB database Spark SQL Row
   *
   * @param orientEdge OrientDB edge
   * @return Apache Spark SQL Row
   */
  def importEdge(orientEdge: Edge): Row = {
    try {
      createEdge(orientEdge)
    }
    catch {
      case e: Exception =>
        throw new RuntimeException(s"Unable to read edge of ID ${orientEdge.getId.toString} from OrientDB graph: ${e.getMessage}")
    }
  }

  /**
   * A method creates Spark SQL Row
   *
   * @param orientEdge OrientDB edge
   * @return Apache Spark SQL Row
   */
  def createEdge(orientEdge: Edge): Row = {

    val row = edgeSchema.fields.map(field => {
      orientEdge.getProperty(field.name): Any
    })
    Row.fromSeq(row.toSeq)
  }

}
