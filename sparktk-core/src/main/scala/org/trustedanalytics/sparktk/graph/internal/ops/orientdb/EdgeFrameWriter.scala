package org.trustedanalytics.sparktk.graph.internal.ops.orientdb

import org.apache.spark.sql.DataFrame
import org.trustedanalytics.sparktk.graph.internal.GraphSchema

/**
 * exports the current frame to OrientDB edges class
 *
 * @param edgeFrame edges data frame
 * @param dbConfig database configurations parameters
 */
class EdgeFrameWriter(edgeFrame: DataFrame, dbConfig: OrientConf) extends Serializable {

  /**
   * exports edges data frame to OrientDB edges class
   *
   * @param batchSize batch size
   * @param edgeTypeColumnName the given column name for edge type
   * @return the number of exported edges
   */
  def exportEdgeFrame(batchSize: Int, edgeTypeColumnName: Option[String] = None): Long = {

    val edgesCountRdd = edgeFrame.mapPartitions(iter => {
      var batchCounter = 0L
      val orientGraph = OrientdbGraphFactory.graphDbConnector(dbConfig)
      try {
        val edgeWriter = new EdgeWriter(orientGraph)
        while (iter.hasNext) {
          val row = iter.next()
          edgeWriter.create(row, edgeTypeColumnName)
          batchCounter += 1
          if (batchCounter % batchSize == 0 && batchCounter != 0) {
            orientGraph.commit()
          }
        }
      }
      catch {
        case e: Exception =>
          orientGraph.rollback()
          throw new RuntimeException(s"Unable to add edges to OrientDB graph: ${e.getMessage}")
      }
      finally {
        orientGraph.shutdown(true, true) //commit the changes and close the graph
      }
      Array(batchCounter).toIterator
    })
    edgesCountRdd.sum().toLong
  }
}
