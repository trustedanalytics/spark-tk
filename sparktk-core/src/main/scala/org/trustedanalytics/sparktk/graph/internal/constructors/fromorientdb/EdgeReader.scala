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
