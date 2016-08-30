package org.trustedanalytics.sparktk.graph.internal.constructors.fromorientdb

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.trustedanalytics.sparktk.graph.internal.ops.orientdb.OrientConf

/**
 * converts edges from OrientDB graph database to Spark RDD
 *
 * @param orientConf OrientDB database configurations
 */
class EdgeFrameReader(orientConf: OrientConf) {

  /**
   * imports edges class from OrientDB and converts it to Spark RDD
   *
   * @param sc Spark context
   * @return RDD of edges
   */
  def importOrientDbEdgeClass(sc: SparkContext): RDD[Row] = {
    val edgeRdd = new OrientDbEdgeRdd(sc, orientConf)
    edgeRdd
  }

}
