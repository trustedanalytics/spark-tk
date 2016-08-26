package org.trustedanalytics.sparktk.graph.internal.constructors.fromorientdb

import com.orientechnologies.orient.core.sql.OCommandSQL
import com.tinkerpop.blueprints.Vertex
import com.tinkerpop.blueprints.impls.orient.OrientDynaElementIterable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{ Partition, SparkContext, TaskContext }
import org.trustedanalytics.sparktk.graph.internal.ops.orientdb.{ OrientdbGraphFactory, OrientConf }

import scala.collection.mutable.ArrayBuffer

/**
 * creates Spark RDDs for the imported vertex classes from OrientDB graph
 *
 * @param sc Spark context
 * @param dbConfigurations OrientDB database configurations
 */
class OrientDbVertexRdd(sc: SparkContext, dbConfigurations: OrientConf) extends RDD[Row](sc, Nil) {

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val graph = OrientdbGraphFactory.graphDbConnector(dbConfigurations)
    val partition = split.asInstanceOf[OrientDbPartition]
    val vertexBuffer = new ArrayBuffer[Row]()
    val schemaReader = new SchemaReader(graph)
    val vertexSchema = schemaReader.importVertexSchema
    val vertices: OrientDynaElementIterable = graph.command(
      new OCommandSQL(s"select from cluster:${partition.clusterId} where @class='${partition.className}'")
    ).execute()
    val vertexIterator = vertices.iterator().asInstanceOf[java.util.Iterator[Vertex]]
    while (vertexIterator.hasNext) {
      val vertexReader = new VertexReader(graph, vertexSchema)
      val vertex = vertexReader.importVertex(vertexIterator.next())
      vertexBuffer += vertex
    }
    vertexBuffer.toIterator
  }

  /**
   * divides OrientDB vertices to partitions, each partition has data from a single cluster and class
   *
   * @return Array of partitions for OrientDB graph vertices to be imported in parallel
   */
  override protected def getPartitions: Array[Partition] = {
    val partitionBuffer = new ArrayBuffer[OrientDbPartition]()
    val graph = OrientdbGraphFactory.graphDbConnector(dbConfigurations)
    val classBaseNames = graph.getVertexBaseType.getName
    val classIterator = graph.getVertexType(classBaseNames).getAllSubclasses.iterator()
    var partitionIdx = 0
    while (classIterator.hasNext) {
      val classLabel = classIterator.next().getName
      val clusterIds = graph.getVertexType(classLabel).getClusterIds
      clusterIds.foreach(id => {
        partitionBuffer += new OrientDbPartition(id, classLabel, partitionIdx)
        partitionIdx += 1
      })
    }
    partitionBuffer.toArray
  }
}
