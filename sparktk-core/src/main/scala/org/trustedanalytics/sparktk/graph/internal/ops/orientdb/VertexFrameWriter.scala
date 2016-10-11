/**
 *  Copyright (c) 2016 Intel Corporation 
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
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
