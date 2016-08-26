package org.trustedanalytics.sparktk.graph.internal.constructors.fromorientdb

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.trustedanalytics.sparktk.graph.internal.ops.orientdb.OrientConf

/**
 * converts vertex class from OrientDB database to RDD of vertices
 *
 * @param dbConfigurations OrientDB configurations
 */
class VertexFrameReader(dbConfigurations: OrientConf) {

  /**
   * imports vertex class from OrientDB to RDD of vertices
   *
   * @return RDD of vertices
   */
  def importOrientDbVertexClass(sc: SparkContext): RDD[Row] = {

    val vertexRdd = new OrientDbVertexRdd(sc, dbConfigurations)
    vertexRdd
  }
}
