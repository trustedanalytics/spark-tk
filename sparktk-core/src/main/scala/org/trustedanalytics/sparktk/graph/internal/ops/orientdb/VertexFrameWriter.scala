package org.trustedanalytics.sparktk.graph.internal.ops.orientdb

import org.apache.spark.sql.DataFrame
import org.trustedanalytics.sparktk.graph.internal.GraphSchema

/**
 * exports data frame to OrientDB vertices class
 *
 * @param vertexFrame vertices data frame
 * @param dbConfig the database configurations parameters
 */
class VertexFrameWriter(vertexFrame: DataFrame, dbConfig: OrientConf) extends Serializable {

  /**
   *
   * @param batchSize batch size
   * @return the number of exported vertices
   */
  def exportVertexFrame(batchSize: Int): Long = {
    val verticesCountRdd = vertexFrame.mapPartitions(iter => {
      var batchCounter = 0L
      val orientGraph = OrientdbGraphFactory.graphDbConnector(dbConfig)
      try {
        while (iter.hasNext) {
          val row = iter.next()
          val vertexWriter = new VertexWriter(orientGraph)
          val vertex = vertexWriter.create(GraphSchema.vertexTypeColumnName, row)
          batchCounter += 1
          if (batchCounter % batchSize == 0 && batchCounter != 0) {
            orientGraph.commit()
          }

        }
      }
      catch {
        case e: Exception =>
          orientGraph.rollback()
          throw new RuntimeException(s"Unable to add vertices to OrientDB graph: ${e.getMessage}")
      }
      finally {
        orientGraph.shutdown(true, true) // commit and close the graph database
      }
      Array(batchCounter).toIterator
    })
    verticesCountRdd.sum().toLong
  }

}
