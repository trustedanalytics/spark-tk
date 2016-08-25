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
    *
    * @param sc spark context
    * @param dbUrl database URI
    * @param userName username
    * @param password password
    * @param rootPassword OrientDB server Password
    * @return spark-tk graph
    */
  def importOrientdbGraph(sc: SparkContext, dbUrl: String, userName: String, password: String, rootPassword: String): Graph = {
    val sqlContext = new SQLContext(sc)
    val orientConf = OrientConf(dbUrl, userName, password, rootPassword)
    val importer = new ImportGraphFunctions(sqlContext)
    new Graph(importer.orientGraphFrame(orientConf))
  }
}
