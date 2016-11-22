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
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.trustedanalytics.sparktk.graph.internal.ops.orientdb.OrientdbConf

/**
 * converts vertex class from OrientDB database to RDD of vertices
 *
 * @param dbConfigurations OrientDB configurations
 * @param dbName database name
 */
class VertexFrameReader(dbConfigurations: OrientdbConf, dbName: String) {

  /**
   * imports vertex class from OrientDB to RDD of vertices
   *
   * @return RDD of vertices
   */
  def importOrientDbVertexClass(sc: SparkContext): RDD[Row] = {

    val vertexRdd = new OrientDbVertexRdd(sc, dbConfigurations, dbName)
    vertexRdd
  }
}
