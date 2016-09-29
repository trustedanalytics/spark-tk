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
 * exports the current frame to OrientDB edges class
 *
 * @param edgeFrame edges data frame
 * @param dbConfig database configurations parameters
 */
class EdgeFrameWriter(edgeFrame: DataFrame, dbConfig: OrientConf) extends Serializable {

  /**
   * exports edges data frame to OrientDB edges class
   * @param batchSize batch size
   * @return the number of edges
   */
  def exportEdgeFrame(batchSize: Int): Long = {

    val edgesCountRdd = edgeFrame.mapPartitions(iter => {
      var batchCounter = 0L
      val orientGraph = OrientdbGraphFactory.graphDbConnector(dbConfig)
      try {
        val edgeWriter = new EdgeWriter(orientGraph)
        while (iter.hasNext) {
          val row = iter.next()
          edgeWriter.create(row, GraphSchema.vertexTypeColumnName)
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
