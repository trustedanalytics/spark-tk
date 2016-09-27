package org.trustedanalytics.sparktk.graph.internal.constructors.fromorientdb

import com.tinkerpop.blueprints.impls.orient._
import org.apache.spark.sql.types.{ DataType, StructField, StructType }
import org.graphframes.GraphFrame
import org.trustedanalytics.sparktk.graph.internal.GraphSchema
import org.trustedanalytics.sparktk.graph.internal.ops.orientdb.DataTypesConverter
/**
 * converts OrientDB graph schema to Spark graph frame schema
 *
 * @param graph OrientDB graph database
 */
class SchemaReader(graph: OrientGraphNoTx) {

  /**
   * imports OrientDB vertex schema to vertex dataframe schema. If OrientDB graph has vertex classes other than
   * the base vertex class "V", it will read each class schema and merge them to get unique schema fields.
   *
   * @return  vertex schema for spark data frame
   */
  def importVertexSchema: StructType = {

    try {
      val vertexTypesList = getVertexClasses.getOrElse(Set(graph.getVertexBaseType.getName))
      val vertexSchema = mergeSchema(vertexTypesList)
      validateVertexSchema(vertexSchema)
      vertexSchema
    }
    catch {
      case e: Exception =>
        throw new RuntimeException(s"Unable to read vertex schema from OrientDB graph: ${e.getMessage}")
    }
  }

  /**
   * Get OrientDB vertex classes names if exists
   *
   * @return vertex class names
   */
  def getVertexClasses: Option[Set[String]] = {
    val classBaseNames = graph.getVertexBaseType.getName
    val vertexClassesIterator = graph.getVertexType(classBaseNames).getAllSubclasses.iterator()
    var vertexTypesBuffer = Set[String]()
    while (vertexClassesIterator.hasNext) {
      vertexTypesBuffer += vertexClassesIterator.next().getName
    }
    if (vertexTypesBuffer.nonEmpty) {
      return Some(vertexTypesBuffer)
    }
    None
  }

  /**
   * Get OrientDB vertex properties based on vertex class name and write each property schema in a StructField
   *
   * @param className OrientDB vertex class name
   * @param orientGraph OrientDB graph instance
   * @return vertex class schema
   */
  def getSchemaPerClass(className: String, orientGraph: OrientGraphNoTx): Set[StructField] = {
    var propertiesBuffer = Set[StructField]()
    val propKeysIterator = graph.getRawGraph.getMetadata.getSchema.getClass(className).properties().iterator()
    while (propKeysIterator.hasNext) {
      val prop = propKeysIterator.next()
      val columnType: DataType = DataTypesConverter.orientdbToSpark(prop.getType)
      if (prop.getName == graphParameters.orientVertexId) propertiesBuffer += new StructField(GraphFrame.ID, columnType)
      else propertiesBuffer += new StructField(prop.getName, columnType)
    }
    propertiesBuffer
  }

  /**
   * Merge different types schemas and returns schema with unique column fields
   *
   * @param classNames OrientDB class names
   * @return dataframe schema
   */
  def mergeSchema(classNames: Set[String]): StructType = {
    var schemaFields = Set[StructField]()
    classNames.foreach(className => {
      schemaFields ++= getSchemaPerClass(className, graph)
    })
    new StructType(schemaFields.toArray)
  }

  /**
   * imports OrientDB edge schema to edge dataframe schema. If OrientDB graph has edge classes other than
   * the base edge class "E", it will read each class schema and merge them to get unique schema fields.
   *
   * @return edge schema for spark data frame
   */
  def importEdgeSchema: StructType = {
    try {
      val edgeTypesList = getEdgeClasses.getOrElse(Set(graph.getEdgeBaseType.getName))
      val edgeSchema = mergeSchema(edgeTypesList)
      validateEdgeSchema(edgeSchema)
      edgeSchema
    }
    catch {
      case e: Exception =>
        throw new RuntimeException(s"Unable to read edge schema from OrientDB graph: ${e.getMessage}")
    }
  }

  /**
   * Get OrientDB edge class names if exists
   *
   * @return edge classes names
   */
  def getEdgeClasses: Option[Set[String]] = {
    val classBaseNames = graph.getEdgeBaseType.getName
    val edgeClassesIterator = graph.getEdgeType(classBaseNames).getAllSubclasses.iterator()
    var edgeTypesBuffer = Set[String]()
    while (edgeClassesIterator.hasNext) {
      edgeTypesBuffer += edgeClassesIterator.next().getName
    }
    if (edgeTypesBuffer.nonEmpty) {
      return Some(edgeTypesBuffer)
    }
    None
  }

  /**
   * validates the imported vertex schema is proper to represent a vertices frame schema
   *
   * @param vertexSchema vertex schema
   */
  def validateVertexSchema(vertexSchema: StructType) = {
    require(vertexSchema.fieldNames.contains(GraphSchema.vertexIdColumnName), s"Schema must contain field named ${GraphSchema.vertexIdColumnName}")
  }

  /**
   * validates the imported edge schema is proper to represent a edges frame schema
   *
   * @param edgeSchema edge schema
   */
  def validateEdgeSchema(edgeSchema: StructType) = {
    require(edgeSchema.fieldNames.contains(GraphSchema.edgeSourceColumnName), s"Schema must contain field named $GraphSchema.edgeSourceColumnName")
    require(edgeSchema.fieldNames.contains(GraphSchema.edgeDestinationColumnName), s"Schema must contain field named $GraphSchema.edgeDestinationColumnName")
  }
}

/**
 * column names are used by OrientDB graph
 */
object graphParameters {

  //the exported graph frame ID
  val orientVertexId = GraphFrame.ID + "_"
}
