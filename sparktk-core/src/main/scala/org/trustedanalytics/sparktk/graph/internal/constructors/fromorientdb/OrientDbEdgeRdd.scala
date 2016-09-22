package org.trustedanalytics.sparktk.graph.internal.constructors.fromorientdb

import com.orientechnologies.orient.core.sql.OCommandSQL
import com.tinkerpop.blueprints.impls.orient.OrientDynaElementIterable
import com.tinkerpop.blueprints.{ Edge => BlueprintsEdge }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{ Partition, SparkContext, TaskContext }
import org.trustedanalytics.sparktk.graph.internal.ops.orientdb.{ OrientdbGraphFactory, OrientConf }

import scala.collection.mutable.ArrayBuffer

/**
 * creates Spark RDDs for the imported edge classes from OrientDB graph
 *
 * @param sc Spark context
 * @param dbConfigurations OrientDB database configurations
 */
class OrientDbEdgeRdd(sc: SparkContext, dbConfigurations: OrientConf) extends RDD[Row](sc, Nil) {

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val graph = OrientdbGraphFactory.graphDbConnector(dbConfigurations)
    val partition = split.asInstanceOf[OrientDbPartition]
    val edgeBuffer = new ArrayBuffer[Row]()
    val schemaReader = new SchemaReader(graph)
    val edgeSchema = schemaReader.importEdgeSchema
    val edges: OrientDynaElementIterable = graph.command(new OCommandSQL(s"select from cluster:${partition.clusterId} where @class='${partition.className}'")).execute()
    val edgeIterator = edges.iterator().asInstanceOf[java.util.Iterator[BlueprintsEdge]]
    while (edgeIterator.hasNext) {
      val edgeReader = new EdgeReader(graph, edgeSchema)
      val edge = edgeReader.importEdge(edgeIterator.next())
      edgeBuffer += edge
    }
    edgeBuffer.toIterator
  }

  /**
   * divides OrientDB edges to partitions based on OrientDB edge class type and cluster ID
   *
   * @return Array of partitions for OrientDB graph edges to be imported in parallel
   */
  override protected def getPartitions: Array[Partition] = {
    val partitionBuffer = new ArrayBuffer[OrientDbPartition]()
    val graph = OrientdbGraphFactory.graphDbConnector(dbConfigurations)
    val schemaReader = new SchemaReader(graph)
    val edgeTypes = schemaReader.getEdgeClasses.getOrElse(Set(graph.getEdgeBaseType.getName))
    var partitionIdx = 0
    edgeTypes.foreach(edgeType => {
      val clusterIds = graph.getEdgeType(edgeType).getClusterIds
      clusterIds.foreach(id => {
        partitionBuffer += new OrientDbPartition(id, edgeType, partitionIdx)
        partitionIdx += 1
      })
    })
    partitionBuffer.toArray
  }
}
