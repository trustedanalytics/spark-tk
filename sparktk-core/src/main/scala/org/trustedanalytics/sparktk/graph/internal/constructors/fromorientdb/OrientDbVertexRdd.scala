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
 * @param dbName database name
 */
class OrientDbVertexRdd(sc: SparkContext, dbConfigurations: OrientConf, dbName:String) extends RDD[Row](sc, Nil) {

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val graph = OrientdbGraphFactory.graphDbConnector(dbConfigurations, dbName)
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
   * divides OrientDB vertices to partitions based on OrientDB vertex class type and cluster ID
   *
   * @return spark partitions for OrientDB graph vertices to be imported in parallel
   */
  override protected def getPartitions: Array[Partition] = {
    val partitionBuffer = new ArrayBuffer[OrientDbPartition]()
    val graph = OrientdbGraphFactory.graphDbConnector(dbConfigurations, dbName)
    val schemaReader = new SchemaReader(graph)
    val vertexTypes = schemaReader.getVertexClasses.getOrElse(Set(graph.getVertexBaseType.getName))
    var partitionIdx = 0
    vertexTypes.foreach(vertexType => {
      val clusterIds = graph.getVertexType(vertexType).getClusterIds
      clusterIds.foreach(id => {
        partitionBuffer += new OrientDbPartition(id, vertexType, partitionIdx)
        partitionIdx += 1
      })
    })
    partitionBuffer.toArray
  }
}
