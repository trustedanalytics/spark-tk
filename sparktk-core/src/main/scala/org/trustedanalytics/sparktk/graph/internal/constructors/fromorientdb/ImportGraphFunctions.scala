package org.trustedanalytics.sparktk.graph.internal.constructors.fromorientdb

import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.graphframes.GraphFrame
import org.trustedanalytics.sparktk.graph.internal.ops.orientdb.{ OrientdbGraphFactory, OrientConf }

/**
 * imports OrientDB graph and converts it to Apache Spark GraphFrame
 *
 * @param sqlContext Apche Spark SQL context
 */
class ImportGraphFunctions(sqlContext: SQLContext) {
  def orientGraphFrame(orientConf: OrientConf): GraphFrame = {
    val orientGraph = OrientdbGraphFactory.graphDbConnector(orientConf)
    val vertexDataFrame = createVertexDataFrame(orientConf, orientGraph)
    val edgeDataFrame: DataFrame = createEdgeDataFrame(orientConf, orientGraph)
    GraphFrame(vertexDataFrame, edgeDataFrame)
  }

  /**
   * converts OrientDB edges class to Spark DataFrame
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
   * converts OrientDB vertices class to Spark DataFrame
   *
   * @param orientConf OrientB database configurations
   * @param orientGraph OrientDB database
   * @return vertices data frame
   */
  def createVertexDataFrame(orientConf: OrientConf, orientGraph: OrientGraphNoTx): DataFrame = {
    val classBaseNames = orientGraph.getVertexBaseType.getName
    val classIterator = orientGraph.getVertexType(classBaseNames).getAllSubclasses.iterator()
    val vertexType = classIterator.next().getName
    val schemaReader = new SchemaReader(orientGraph)
    val vertexSchema = schemaReader.importVertexSchema(vertexType)
    val vertexFrameReader = new VertexFrameReader(orientConf)
    val vertexFrame = vertexFrameReader.importOrientDbVertexClass(sqlContext.sparkContext)
    val vertexDataFrame = sqlContext.createDataFrame(vertexFrame, vertexSchema)
    vertexDataFrame
  }
}
