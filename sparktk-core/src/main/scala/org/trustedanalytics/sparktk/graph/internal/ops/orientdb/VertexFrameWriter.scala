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
   * exports vertex dataframe to OrientDB
   * @param batchSize batch size
   * @param vertexTypeColumnName the given column name for vertex type
   * @return the number of exported vertices
   */
  def exportVertexFrame(batchSize: Int, vertexTypeColumnName: Option[String] = None): Long = {
    val verticesCountRdd = vertexFrame.mapPartitions(iter => {
      var batchCounter = 0L
      val orientGraph = OrientdbGraphFactory.graphDbConnector(dbConfig)
      try {
        val vertexWriter = new VertexWriter(orientGraph)
        while (iter.hasNext) {
          val row = iter.next()
          val vertex = vertexWriter.create(row, vertexTypeColumnName)
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
