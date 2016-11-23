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

import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx
import org.apache.spark.SparkContext
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.graphframes.GraphFrame
import org.trustedanalytics.sparktk.graph.Graph
import org.trustedanalytics.sparktk.graph.internal.GraphSchema
import org.trustedanalytics.sparktk.graph.internal.ops.orientdb.{ OrientdbGraphFactory, OrientdbConf }

/**
 * imports graph from OrientDB to spark-tk
 */
object ImportFromOrientdb {

  /**
   * Import OrientDB graph to spark-tk graph
   *
   * @param sc: Spark context
   * @param orientConf configuration settings for the OrientDB connection
   * @param dbName the database name
   * @return spark-tk graph
   */
  def importOrientdbGraph(sc: SparkContext, orientConf: OrientdbConf, dbName: String, dbProperties: Option[Map[String, Any]]): Graph = {
    implicit val sqlContext = new SQLContext(sc)
    val orientGraph = OrientdbGraphFactory.graphDbConnector(orientConf, dbName, dbProperties)
    val vertexDataFrame = createVertexDataFrame(orientConf, orientGraph, dbName)
    GraphSchema.validateSchemaForVerticesFrame(vertexDataFrame)
    val edgeDataFrame: DataFrame = createEdgeDataFrame(orientConf, orientGraph, dbName)
    GraphSchema.validateSchemaForEdgesFrame(edgeDataFrame)
    val graphFrame = GraphFrame(vertexDataFrame, edgeDataFrame)
    new Graph(graphFrame)
  }

  /**
   * imports OrientDB edge class to data frame of edges
   *
   * @param orientConf OrientB database configurations
   * @param orientGraph OrientDB database
   * @param dbName database name
   * @return edges data frame
   */
  def createEdgeDataFrame(orientConf: OrientdbConf, orientGraph: OrientGraphNoTx, dbName: String)(implicit sqlContext: SQLContext): DataFrame = {
    val schemaReader = new SchemaReader(orientGraph)
    val edgeSchema = schemaReader.importEdgeSchema
    val edgeFrameReader = new EdgeFrameReader(orientConf, dbName)
    val edgeFrame = edgeFrameReader.importOrientDbEdgeClass(sqlContext.sparkContext)
    val edgeDataFrame = sqlContext.createDataFrame(edgeFrame, edgeSchema)
    edgeDataFrame
  }

  /**
   * imports OrientDB vertex class to data frame of vertices
   *
   * @param orientConf OrientB database configurations
   * @param orientGraph OrientDB database
   * @param dbName database name
   * @return vertices data frame
   */
  def createVertexDataFrame(orientConf: OrientdbConf, orientGraph: OrientGraphNoTx, dbName: String)(implicit sqlContext: SQLContext): DataFrame = {

    val schemaReader = new SchemaReader(orientGraph)
    val vertexSchema = schemaReader.importVertexSchema
    val vertexFrameReader = new VertexFrameReader(orientConf, dbName)
    val vertexFrame = vertexFrameReader.importOrientDbVertexClass(sqlContext.sparkContext)
    val vertexDataFrame = sqlContext.createDataFrame(vertexFrame, vertexSchema)
    vertexDataFrame
  }
}
