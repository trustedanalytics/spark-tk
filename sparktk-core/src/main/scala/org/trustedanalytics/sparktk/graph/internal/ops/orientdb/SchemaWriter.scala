package org.trustedanalytics.sparktk.graph.internal.ops.orientdb

import com.tinkerpop.blueprints.{ Vertex, Parameter }
import com.tinkerpop.blueprints.impls.orient.{ OrientEdgeType, OrientVertexType, OrientGraphNoTx }
import org.apache.spark.sql.{ Row, DataFrame }
import org.apache.spark.sql.types.{ StructField, StructType }
import org.graphframes.GraphFrame
import org.trustedanalytics.sparktk.graph.internal.GraphSchema

import scala.collection.mutable.ArrayBuffer

/**
 * exports the graph frame schema to OrientDB graph schema
 */
class SchemaWriter extends Serializable {

  /**
   * exports the schema of the vertex dataframe to OrientDB. if a column is specified for vertex types,
   * it creates vertex classes based on the given types and exports the corresponding schema, otherwise all vertices are exported to OrientDB base vertex class "V" or to
   *
   * @param vertexFrame vertex dataframe
   * @param orientGraph OrientDB graph
   * @param vertexTypeColumnName vertex type column name
   */
  def vertexSchema(vertexFrame: DataFrame, orientGraph: OrientGraphNoTx, vertexTypeColumnName: Option[String] = None) = {
    try {
      if (vertexTypeColumnName.isDefined) {
        val aggRdd = inferSchema(vertexFrame, vertexTypeColumnName.get)
        aggRdd.foreach(vertexType => {
          val (name, schema) = vertexType
          val orientVertexType = getOrCreateVertexType(name, orientGraph)
          exportVertexColumnsSchema(orientVertexType, schema)
          orientGraph.createKeyIndex(exportGraphParam.vertexId, classOf[Vertex], new Parameter("type", "UNIQUE"), new Parameter("class", name))
        })
      }
      else {
        exportVertexColumnsSchema(orientGraph.getVertexBaseType, vertexFrame.schema.fields.toSet)
        orientGraph.createKeyIndex(exportGraphParam.vertexId, classOf[Vertex], new Parameter("type", "UNIQUE"))
      }
    }
    catch {
      case e: Exception =>
        orientGraph.rollback()
        throw new RuntimeException(s"Unable to create the vertex schema:${e.getMessage}", e)
    }
  }

  /**
   * exports the schema of the edge dataframe to OrientDB. if a column is specified for edge types,
   * it creates edge classes based on the given types and exports the corresponding schema, otherwise all edges are exported to OrientDB base edge class "E".
   *
   * @param edgeFrame edge dataframe
   * @param orientGraph OrientDB graph
   * @param edgeTypeColumnName vertex type column name
   */
  def edgeSchema(edgeFrame: DataFrame, orientGraph: OrientGraphNoTx, edgeTypeColumnName: Option[String] = None) {
    try {
      if (edgeTypeColumnName.isDefined) {
        val aggRdd = inferSchema(edgeFrame, edgeTypeColumnName.get)
        aggRdd.foreach(edgeType => {
          val (name, schema) = edgeType
          val orientEdgeType = getOrCreateEdgeType(name, orientGraph)
          exportEdgeColumnsSchema(orientEdgeType, schema)
        })
      }
      else {
        exportEdgeColumnsSchema(orientGraph.getEdgeBaseType, edgeFrame.schema.fields.toSet)
      }
    }
    catch {
      case e: Exception =>
        orientGraph.rollback()
        throw new RuntimeException(s"Unable to create the edge schema: ${e.getMessage}")
    }
  }

  /**
   * exports the edge dataframe columns schema to OrientDB as edge properties
   *
   * @param orientEdgeType OrientDB edge type
   * @param schema columns schema
   */
  def exportEdgeColumnsSchema(orientEdgeType: OrientEdgeType, schema: Set[StructField]) {
    val fieldsIterator = schema.iterator
    while (fieldsIterator.hasNext) {
      val field = fieldsIterator.next()
      val orientColumnDataType = DataTypesConverter.sparkToOrientdb(field.dataType)
      orientEdgeType.createProperty(field.name, orientColumnDataType)
    }
  }

  /**
   * infer schema from the dataframe based on the given vertex or edge types
   *
   * @param frame vertex or edge dataframe
   * @param columnName the specified vertex or edge type column name
   * @return vertex or edge schema based on the type
   */
  def inferSchema(frame: DataFrame, columnName: String): Array[(String, Set[StructField])] = {
    val rdd = frame.rdd.keyBy[String](row => row.getAs[String](columnName))
    rdd.aggregateByKey(Set.empty[StructField])(inferSchemaFromRow, mergeSchema).collect()
  }

  /**
   * infer the row schema
   *
   * @param fields schema
   * @param row  vertex or edge row
   * @return row schema
   */
  def inferSchemaFromRow(fields: Set[StructField], row: Row): Set[StructField] = {
    var fieldSet = fields
    val rowSchemaIterator = row.schema.iterator
    while (rowSchemaIterator.hasNext) {
      val columnField = rowSchemaIterator.next()
      if (row.getAs[String](columnField.name) != null) {
        fieldSet += columnField
      }
    }
    fieldSet
  }

  /**
   * merges row schemas for the same vertex or edge type.
   *
   * @param schema1 row schema
   * @param schema2 row schema
   * @return row schema with unique column names
   */
  def mergeSchema(schema1: Set[StructField], schema2: Set[StructField]): Set[StructField] = {
    schema1 ++ schema2
  }

  /**
   * exports vertex type to OrientDB vertex class
   *
   * @param vertexType vertex type or name
   * @param orientGraph OrientDB graph
   * @return OrientDB vertex type
   */
  def getOrCreateVertexType(vertexType: String, orientGraph: OrientGraphNoTx): OrientVertexType = {
    val orientType = if (orientGraph.getVertexType(vertexType) == null) {
      orientGraph.createVertexType(vertexType)
    }
    else {
      orientGraph.getVertexType(vertexType)
    }
    orientType
  }

  /**
   * exports edge type to OrientDB edge class
   *
   * @param edgeType edge type or name
   * @param orientGraph OrientDB graph
   * @return OrientDB edge type
   */
  def getOrCreateEdgeType(edgeType: String, orientGraph: OrientGraphNoTx): OrientEdgeType = {
    val orientType = if (orientGraph.getEdgeType(edgeType) == null) {
      orientGraph.createEdgeType(edgeType)
    }
    else {
      orientGraph.getEdgeType(edgeType)
    }
    orientType
  }

  /**
   * exports the vertex dataframe columns schema to OrientDB as vertex properties
   *
   * @param orientVertexType OrientDB vertex type
   * @param schema schema per type
   */
  def exportVertexColumnsSchema(orientVertexType: OrientVertexType, schema: Set[StructField]) {
    val fieldsIterator = schema.iterator
    while (fieldsIterator.hasNext) {
      val field = fieldsIterator.next()
      val orientColumnDataType = DataTypesConverter.sparkToOrientdb(field.dataType)
      if (field.name == GraphFrame.ID) {
        orientVertexType.createProperty(exportGraphParam.vertexId, orientColumnDataType)
      }
      else {
        orientVertexType.createProperty(field.name, orientColumnDataType)
      }
    }
  }
}

/**
 * hard coded parameters
 */
object exportGraphParam {
  val vertexId = GraphFrame.ID + "_"
}
