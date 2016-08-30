package org.trustedanalytics.sparktk.graph.internal.constructors.fromorientdb

import com.tinkerpop.blueprints.Edge
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

/**
 * converts OrientDB edge to Spark SQL Row
 *
 * @param graph      OrientDB graph
 * @param edgeSchema schema of edges data frame
 */
class EdgeReader(graph: OrientGraphNoTx, edgeSchema: StructType) {

  /**
   * imports OrientDB edge from OrientDB database a graph frame edge
   *
   * @param orientEdge OrientDB edge
   * @return graph frame edge
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
   * creates graph frame edge
   *
   * @param orientEdge OrientDB edge
   * @return graph frame edge
   */
  def createEdge(orientEdge: Edge): Row = {

    val row = edgeSchema.fields.map(field => {
      orientEdge.getProperty(field.name): Any
    })
    Row.fromSeq(row.toSeq)
  }

}
