package org.trustedanalytics.sparktk.graph.internal.constructors.fromorientdb

import org.apache.spark.Partition

/**
 * parallelizes the import from OrientDB so that we read data from a single cluster and class in each partition
 * @param clusterId OrientDB cluster ID
 * @param className OrientDB class name
 * @param idx partition index
 */
case class OrientDbPartition(clusterId: Int, className: String, idx: Int) extends Partition {
  override def index: Int = {
    idx
  }
}
