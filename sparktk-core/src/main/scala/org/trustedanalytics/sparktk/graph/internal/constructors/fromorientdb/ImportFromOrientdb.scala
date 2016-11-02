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

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.trustedanalytics.sparktk.graph.Graph
import org.trustedanalytics.sparktk.graph.internal.ops.orientdb.OrientConf

/**
 * imports graph from OrientDB to spark-tk
 */
object ImportFromOrientdb {

  /**
   * imports OrientDB graph to spark-tk graph
   * @param sc spark context
   * @param orientConf OrientDB configurations
   * @param dbName the database name
   * @return spark-tk graph
   */
  def importOrientdbGraph(sc: SparkContext, orientConf: OrientConf, dbName:String): Graph = {
    val sqlContext = new SQLContext(sc)
    val importer = new ImportGraphFunctions(sqlContext)
    new Graph(importer.orientGraphFrame(orientConf, dbName))
  }
}
