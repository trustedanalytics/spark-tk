package org.trustedanalytics.sparktk.graph.internal.constructors.fromorientdb

import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.graphframes.GraphFrame
import org.trustedanalytics.sparktk.graph.internal.GraphSchema
import org.trustedanalytics.sparktk.graph.internal.ops.orientdb.{ OrientdbGraphFactory, OrientConf }

/**
 * imports OrientDB graph and converts it to Apache Spark GraphFrame
 *
 * @param sqlContext Spark SQL context
 */
class ImportGraphFunctions(sqlContext: SQLContext) {
  def orientGraphFrame(orientConf: OrientConf): GraphFrame = {
    val orientGraph = OrientdbGraphFactory.graphDbConnector(orientConf)
    val vertexDataFrame = createVertexDataFrame(orientConf, orientGraph)
    GraphSchema.validateSchemaForVerticesFrame(vertexDataFrame)
    val edgeDataFrame: DataFrame = createEdgeDataFrame(orientConf, orientGraph)
    GraphSchema.validateSchemaForEdgesFrame(edgeDataFrame)
    GraphFrame(vertexDataFrame, edgeDataFrame)
  }

  /**
   * imports OrientDB edge class to data frame of edges
   *
   * @param orientConf OrientB database configurations
   * @param orientGraph OrientDB database
   * @return edges data frame
   */
  def createEdgeDataFrame(orientConf: OrientConf, orientGraph: OrientGraphNoTx): DataFrame = {
    val schemaReader = new SchemaReader(orientGraph)
    val edgeSchema = schemaReader.importEdgeSchema
    val edgeFrameReader = new EdgeFrameReader(orientConf)
    val edgeFrame = edgeFrameReader.importOrientDbEdgeClass(sqlContext.sparkContext)
    val edgeDataFrame = sqlContext.createDataFrame(edgeFrame, edgeSchema)
    edgeDataFrame
  }

  /**
   * imports OrientDB vertex class to data frame of vertices
   *
   * @param orientConf OrientB database configurations
   * @param orientGraph OrientDB database
   * @return vertices data frame
   */
  def createVertexDataFrame(orientConf: OrientConf, orientGraph: OrientGraphNoTx): DataFrame = {

    val schemaReader = new SchemaReader(orientGraph)
    val vertexSchema = schemaReader.importVertexSchema
    val vertexFrameReader = new VertexFrameReader(orientConf)
    val vertexFrame = vertexFrameReader.importOrientDbVertexClass(sqlContext.sparkContext)
    val vertexDataFrame = sqlContext.createDataFrame(vertexFrame, vertexSchema)
    vertexDataFrame
  }
}
