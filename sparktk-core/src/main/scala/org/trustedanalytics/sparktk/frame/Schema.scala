package org.trustedanalytics.sparktk.frame

import java.util.{ ArrayList => JArrayList }

import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.commons.lang.{ StringUtils => CommonsStringUtils }
import org.trustedanalytics.sparktk.frame.DataTypes.DataType
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd

import scala.reflect.runtime.universe._

/**
 * Column - this is a nicer wrapper for columns than just tuples
 *
 * @param name the column name
 * @param dataType the type
 */
case class Column(name: String, dataType: DataType) {
  require(name != null, "column name is required")
  require(dataType != null, "column data type is required")
  require(name != "", "column name can't be empty")
  require(Column.isValidColumnName(name), "column name must be alpha-numeric with valid symbols")
}

object Column {
  /**
   * Create Column given column name and scala type
   *
   * @param name Column name
   * @param dtype runtime type (using scala reflection)
   * @return Column
   */
  def apply(name: String, dtype: reflect.runtime.universe.Type): Column = {
    require(name != null, "column name is required")
    require(dtype != null, "column data type is required")
    require(name != "", "column name can't be empty")
    require(Column.isValidColumnName(name), "column name must be alpha-numeric with valid symbols")

    Column(name,
      dtype match {
        case t if t <:< definitions.IntTpe => DataTypes.int32
        case t if t <:< definitions.LongTpe => DataTypes.int64
        case t if t <:< definitions.FloatTpe => DataTypes.float32
        case t if t <:< definitions.DoubleTpe => DataTypes.float64
        case _ => DataTypes.string
      })
  }

  /**
   * Check if the column name is valid
   *
   * @param str Column name
   * @return Boolean indicating if the column name was valid
   */
  def isValidColumnName(str: String): Boolean = {
    for (c <- str.iterator) {
      if (c <= 0x20)
        return false
    }
    true
  }
}

///**
// * Extra schema if this is a vertex frame
// */
//case class VertexSchema(columns: List[Column] = List[Column](), label: String, idColumnName: Option[String] = None) extends GraphElementSchema {
//  require(hasColumnWithType(GraphSchema.vidProperty, DataTypes.int64), "schema did not have int64 _vid column: " + columns)
//  require(hasColumnWithType(GraphSchema.labelProperty, DataTypes.str), "schema did not have string _label column: " + columns)
//  if (idColumnName != null) {
//    //require(hasColumn(vertexSchema.get.idColumnName), s"schema must contain vertex id column ${vertexSchema.get.idColumnName}")
//  }
//
//  /**
//   * If the id column name had already been defined, use that name, otherwise use the supplied name
//   * @param nameIfNotAlreadyDefined name to use if not defined
//   * @return the name to use
//   */
//  def determineIdColumnName(nameIfNotAlreadyDefined: String): String = {
//    idColumnName.getOrElse(nameIfNotAlreadyDefined)
//  }
//
//  /**
//   * Rename a column
//   * @param existingName the old name
//   * @param newName the new name
//   * @return the updated schema
//   */
//  override def renameColumn(existingName: String, newName: String): Schema = {
//    renameColumns(Map(existingName -> newName))
//  }
//
//  /**
//   * Renames several columns
//   * @param names oldName -> newName
//   * @return new renamed schema
//   */
//  override def renameColumns(names: Map[String, String]): Schema = {
//    val newSchema = super.renameColumns(names)
//    val newIdColumn = idColumnName match {
//      case Some(id) => Some(names.getOrElse(id, id))
//      case _ => idColumnName
//    }
//    new VertexSchema(newSchema.columns, label, newIdColumn)
//  }
//
//  override def copy(columns: List[Column]): VertexSchema = {
//    new VertexSchema(columns, label, idColumnName)
//  }
//
//  def copy(idColumnName: Option[String]): VertexSchema = {
//    new VertexSchema(columns, label, idColumnName)
//  }
//
//  override def dropColumn(columnName: String): Schema = {
//    if (idColumnName.isDefined) {
//      require(idColumnName.get != columnName, s"The id column is not allowed to be dropped: $columnName")
//    }
//
//    if (GraphSchema.vertexSystemColumnNames.contains(columnName)) {
//      throw new IllegalArgumentException(s"$columnName is a system column that is not allowed to be dropped")
//    }
//
//    super.dropColumn(columnName)
//  }
//
//}
//
///**
// * Extra schema if this is an edge frame
// * @param label the label for this edge list
// * @param srcVertexLabel the src "type" of vertices this edge connects
// * @param destVertexLabel the destination "type" of vertices this edge connects
// * @param directed true if edges are directed, false if they are undirected
// */
//case class EdgeSchema(columns: List[Column] = List[Column](), label: String, srcVertexLabel: String, destVertexLabel: String, directed: Boolean = false) extends GraphElementSchema {
//  require(hasColumnWithType(GraphSchema.edgeProperty, DataTypes.int64), "schema did not have int64 _eid column: " + columns)
//  require(hasColumnWithType(GraphSchema.srcVidProperty, DataTypes.int64), "schema did not have int64 _src_vid column: " + columns)
//  require(hasColumnWithType(GraphSchema.destVidProperty, DataTypes.int64), "schema did not have int64 _dest_vid column: " + columns)
//  require(hasColumnWithType(GraphSchema.labelProperty, DataTypes.str), "schema did not have string _label column: " + columns)
//
//  override def copy(columns: List[Column]): EdgeSchema = {
//    new EdgeSchema(columns, label, srcVertexLabel, destVertexLabel, directed)
//  }
//
//  override def dropColumn(columnName: String): Schema = {
//
//    if (GraphSchema.edgeSystemColumnNamesSet.contains(columnName)) {
//      throw new IllegalArgumentException(s"$columnName is a system column that is not allowed to be dropped")
//    }
//
//    super.dropColumn(columnName)
//  }
//}

/**
 * Schema for a data frame. Contains the columns with names and data types.
 *
 * @param columns the columns in the data frame
 */
case class FrameSchema(columns: Seq[Column] = Vector[Column]()) extends Schema {

  override def copy(columns: Seq[Column]): FrameSchema = {
    new FrameSchema(columns)
  }

}

/**
 * Common interface for Vertices and Edges
 */
trait GraphElementSchema extends Schema {

  /** Vertex or Edge label */
  def label: String

}

object SchemaHelper {

  val config = ConfigFactory.load(this.getClass.getClassLoader)

  lazy val defaultInferSchemaSampleSize = config.getInt("trustedanalytics.sparktk.frame.schema.infer-schema-sample-size")

  /**
   * Appends letter to the conflicting columns
   *
   * @param left list to which letter needs to be appended
   * @param right list of columns which might conflict with the left list
   * @param appendLetter letter to be appended
   * @return Renamed list of columns
   */
  def funcAppendLetterForConflictingNames(left: List[Column],
                                          right: List[Column],
                                          appendLetter: String,
                                          ignoreColumns: Option[List[Column]] = None): Seq[Column] = {

    var leftColumnNames = left.map(column => column.name)
    val rightColumnNames = right.map(column => column.name)
    val ignoreColumnNames = ignoreColumns.map(column => column.map(_.name)).getOrElse(List(""))

    left.map { column =>
      if (ignoreColumnNames.contains(column.name))
        column
      else if (right.map(i => i.name).contains(column.name)) {
        var name = column.name + "_" + appendLetter
        while (leftColumnNames.contains(name) || rightColumnNames.contains(name)) {
          name = name + "_" + appendLetter
        }
        leftColumnNames = leftColumnNames ++ List(name)
        Column(name, column.dataType)
      }
      else
        column
    }
  }

  /**
   * Join two lists of columns.
   *
   * Join resolves naming conflicts when both left and right columns have same column names
   *
   * @param leftColumns columns for the left side
   * @param rightColumns columns for the right side
   * @return Combined list of columns
   */
  def join(leftColumns: Seq[Column], rightColumns: Seq[Column]): Seq[Column] = {

    val left = funcAppendLetterForConflictingNames(leftColumns.toList, rightColumns.toList, "L")
    val right = funcAppendLetterForConflictingNames(rightColumns.toList, leftColumns.toList, "R")

    left ++ right
  }

  /**
   * Resolve name conflicts (alias) in frame before sending it to spark data frame join
   * @param left Left Frame
   * @param right Right Frame
   * @param joinColumns List of Join Columns (common to both frames)
   * @return Aliased Left and Right Frame
   */
  def resolveColumnNamesConflictForJoin(left: FrameRdd, right: FrameRdd, joinColumns: List[Column]): (FrameRdd, FrameRdd) = {
    val leftColumns = left.schema.columns.toList
    val rightColumns = right.schema.columns.toList
    val renamedLeftColumns = funcAppendLetterForConflictingNames(leftColumns, rightColumns, "L", Some(joinColumns))
    val renamedRightColumns = funcAppendLetterForConflictingNames(rightColumns, leftColumns, "R", Some(joinColumns))

    def createRenamingColumnMap(f: FrameRdd, newColumns: Seq[Column]): FrameRdd = {
      val oldColumnNames = f.schema.columnNames
      val newColumnNames = newColumns.map(_.name)
      f.selectColumnsWithRename(oldColumnNames.zip(newColumnNames).toMap)
    }

    (createRenamingColumnMap(left, renamedLeftColumns), createRenamingColumnMap(right, renamedRightColumns))
  }

  def checkValidColumnsExistAndCompatible(leftFrame: FrameRdd, rightFrame: FrameRdd, leftColumns: Seq[String], rightColumns: Seq[String]) = {
    //Check left join column is compatiable with right join column
    (leftColumns zip rightColumns).foreach {
      case (leftJoinCol, rightJoinCol) => require(DataTypes.isCompatibleDataType(
        leftFrame.schema.columnDataType(leftJoinCol),
        rightFrame.schema.columnDataType(rightJoinCol)),
        "Join columns must have compatible data types")
    }
  }
  /**
   *  Creates Frame schema from input parameters
   *
   * @param dataColumnNames list of column names for the output schema
   * @param dType uniform data type to be applied to all columns if outputVectorLength is not specified
   * @param outputVectorLength optional parameter for a vector datatype
   * @return a new FrameSchema
   */
  def create(dataColumnNames: Seq[String],
             dType: DataType,
             outputVectorLength: Option[Long] = None): FrameSchema = {
    val outputColumns = outputVectorLength match {
      case Some(length) => List(Column(dataColumnNames.head, DataTypes.vector(length)))
      case _ => dataColumnNames.map(name => Column(name, dType))
    }
    FrameSchema(outputColumns.toVector)
  }

  def pythonToScala(pythonSchema: JArrayList[JArrayList[String]]): Schema = {
    import scala.collection.JavaConverters._
    val columns = pythonSchema.asScala.map { item =>
      val list = item.asScala.toList
      require(list.length == 2, "Schema entries must be tuples of size 2 (name, dtype)")
      Column(list.head, DataTypes.toDataType(list(1)))
    }.toVector
    FrameSchema(columns)
  }

  def scalaToPython(scalaSchema: FrameSchema): JArrayList[JArrayList[String]] = {
    val pythonSchema = new JArrayList[JArrayList[String]]()
    scalaSchema.columns.map {
      case Column(name, dtype) =>
        val c = new JArrayList[String]()
        c.add(name)
        c.add(dtype.toString)
        pythonSchema.add(c)
    }
    pythonSchema
  }

  private def mergeType(dataTypeA: DataType, dataTypeB: DataType): DataType = {
    val numericTypes = List[DataType](DataTypes.float64, DataTypes.float32, DataTypes.int64, DataTypes.int32)

    if (dataTypeA.equalsDataType(dataTypeB))
      dataTypeA
    else if (dataTypeA == DataTypes.string || dataTypeB == DataTypes.string)
      DataTypes.string
    else if (numericTypes.contains(dataTypeA) && numericTypes.contains(dataTypeB)) {
      if (numericTypes.indexOf(dataTypeA) > numericTypes.indexOf(dataTypeB))
        dataTypeB
      else
        dataTypeA
    }
    else if (dataTypeA.isVector && dataTypeB.isVector) {
      throw new RuntimeException(s"Vectors must all be the same length (found vectors with length " +
        s"${dataTypeA.asInstanceOf[DataTypes.vector].length} and ${dataTypeB.asInstanceOf[DataTypes.vector].length}).")
    }
    else
      // Unable to merge types, default to use a string
      DataTypes.string
  }

  private def mergeTypes(dataTypesA: Vector[DataType], dataTypesB: Vector[DataType]): Vector[DataType] = {
    if (dataTypesA.length != dataTypesB.length)
      throw new RuntimeException(s"Rows are not the same length (${dataTypesA.length} and ${dataTypesB.length}).")

    dataTypesA.zipWithIndex.map {
      case (dataTypeA, index) => mergeType(dataTypeA, dataTypesB(index))
    }
  }

  private def inferDataTypes(row: Row): Vector[DataType] = {
    row.toSeq.map(value => {
      value match {
        case null => DataTypes.int32
        case i: Int => DataTypes.int32
        case l: Long => DataTypes.int64
        case f: Float => DataTypes.float32
        case d: Double => DataTypes.float64
        case s: String => DataTypes.string
        case l: List[_] => DataTypes.vector(l.length)
        case a: Array[_] => DataTypes.vector(a.length)
        case s: Seq[_] => DataTypes.vector(s.length)
      }
    }).toVector
  }

  /**
   * Looks at the first x number of rows (depending on the sampleSize) to determine the schema of the data set.
   *
   * @param data RDD of data
   * @return Schema inferred from the data
   */
  def inferSchema(data: RDD[Row], sampleSize: Option[Int] = None, columnNames: Option[List[String]] = None): Schema = {

    val sampleSet = data.take(math.min(data.count.toInt, sampleSize.getOrElse(defaultInferSchemaSampleSize)))

    val dataTypes = sampleSet.foldLeft(inferDataTypes(sampleSet.head)) {
      case (v: Vector[DataType], r: Row) => mergeTypes(v, inferDataTypes(r))
    }

    val schemaColumnNames = columnNames.getOrElse(List[String]())

    val columns = dataTypes.zipWithIndex.map {
      case (dataType, index) =>
        val columnName = if (schemaColumnNames.length > index) schemaColumnNames(index) else "C" + index
        Column(columnName, dataType)
    }

    FrameSchema(columns)
  }

  /**
   * Return if list of schema can be merged. Throw exception on name conflicts
   * @param schema List of schema to be merged
   * @return true if schemas can be merged, false otherwise
   */
  def validateIsMergeable(schema: Schema*): Boolean = {
    def merge(schema_l: Schema, schema_r: Schema): Schema = {
      require(schema_l.columnNames.intersect(schema_r.columnNames).isEmpty, "Schemas have conflicting column names." +
        s" Please rename before merging. Left Schema: ${schema_l.columnNamesAsString} Right Schema: ${schema_r.columnNamesAsString}")
      FrameSchema(schema_l.columns ++ schema_r.columns)
    }
    schema.reduce(merge)
    true
  }

}

/**
 * Schema for a data frame. Contains the columns with names and data types.
 */
trait Schema {

  val columns: Seq[Column]

  require(columns != null, "columns must not be null")
  require({
    val distinct = columns.map(_.name).distinct
    distinct.length == columns.length
  }, s"invalid schema, column names cannot be duplicated: $columns")

  private lazy val namesToIndices: Map[String, Int] = (for ((col, i) <- columns.zipWithIndex) yield (col.name, i)).toMap

  def copy(columns: Seq[Column]): Schema

  def columnNames: Seq[String] = {
    columns.map(col => col.name)
  }

  /**
   * True if this schema contains the supplied columnName
   */
  def hasColumn(columnName: String): Boolean = {
    namesToIndices.contains(columnName)
  }

  /**
   * True if this schema contains all of the supplied columnNames
   */
  //  def hasColumns(columnNames: Seq[String]): Boolean = {
  //    columnNames.forall(hasColumn)
  //  }

  def hasColumns(columnNames: Iterable[String]): Boolean = {
    columnNames.forall(hasColumn)
  }

  /**
   * True if this schema contains the supplied columnName with the given dataType
   */
  def hasColumnWithType(columnName: String, dataType: DataType): Boolean = {
    hasColumn(columnName) && column(columnName).dataType.equalsDataType(dataType)
  }

  /**
   * Validate that the list of column names provided exist in this schema
   * throwing an exception if any does not exist.
   */
  def validateColumnsExist(columnNames: Iterable[String]): Iterable[String] = {
    columnIndices(columnNames.toSeq)
    columnNames
  }

  /**
   * Validate that all columns are of numeric data type
   */
  def requireColumnsOfNumericPrimitives(columnNames: Iterable[String]) = {
    columnNames.foreach(columnName => {
      require(hasColumn(columnName), s"column $columnName was not found")
      require(columnDataType(columnName).isNumerical, s"column $columnName should be of type numeric")
    })
  }

  /**
   * Validate that a column exists, and has the expected data type
   */
  def requireColumnIsType(columnName: String, dataType: DataType): Unit = {
    require(hasColumn(columnName), s"column $columnName was not found")
    require(columnDataType(columnName).equalsDataType(dataType), s"column $columnName should be of type $dataType")
  }

  /**
   * Validate that a column exists, and has the expected data type by supplying a custom checker, like isVectorDataType
   */
  def requireColumnIsType(columnName: String, dataTypeChecker: DataType => Boolean): Unit = {
    require(hasColumn(columnName), s"column $columnName was not found")
    val colDataType = columnDataType(columnName)
    require(dataTypeChecker(colDataType), s"column $columnName has bad type $colDataType")
  }

  def requireColumnIsNumerical(columnName: String): Unit = {
    val colDataType = columnDataType(columnName)
    require(colDataType.isNumerical, s"Column $columnName was not numerical. Expected a numerical data type, but got $colDataType.")
  }

  /**
   * Either single column name that is a vector or a list of columns that are numeric primitives
   * that can be converted to a vector.
   *
   * List cannot be empty
   */
  def requireColumnsAreVectorizable(columnNames: Seq[String]): Unit = {
    require(columnNames.nonEmpty, "single vector column, or one or more numeric columns required")
    columnNames.foreach { c => require(CommonsStringUtils.isNotEmpty(c), "data columns names cannot be empty") }
    if (columnNames.size > 1) {
      requireColumnsOfNumericPrimitives(columnNames)
    }
    else {
      val colDataType = columnDataType(columnNames.head)
      require(colDataType.isVector || colDataType.isNumerical, s"column ${columnNames.head} should be of type numeric")
    }
  }

  /**
   * Column names as comma separated list in a single string
   * (useful for error messages, etc)
   */
  def columnNamesAsString: String = {
    columnNames.mkString(", ")
  }

  override def toString: String = columns.mkString(", ")

  // TODO: add a rename column method, since renaming columns shows up in Edge and Vertex schema it is more complicated

  /**
   * get column index by column name
   *
   * Throws exception if not found, check first with hasColumn()
   *
   * @param columnName name of the column to find index
   */
  def columnIndex(columnName: String): Int = {
    val index = columns.indexWhere(column => column.name == columnName, 0)
    if (index == -1)
      throw new IllegalArgumentException(s"Invalid column name $columnName provided, please choose from: " + columnNamesAsString)
    else
      index
  }

  /**
   * Retrieve list of column index based on column names
   *
   * @param columnNames input column names
   */
  def columnIndices(columnNames: Seq[String]): Seq[Int] = {
    if (columnNames.isEmpty)
      columns.indices.toList
    else {
      columnNames.map(columnName => columnIndex(columnName))
    }
  }

  /**
   * Copy a subset of columns into a new Schema
   *
   * @param columnNames the columns to keep
   * @return the new Schema
   */
  def copySubset(columnNames: Seq[String]): Schema = {
    val indices = columnIndices(columnNames)
    val columnSubset = indices.map(i => columns(i)).toVector
    copy(columnSubset)
  }

  /**
   * Produces a renamed subset schema from this schema
   *
   * @param columnNamesWithRename rename mapping
   * @return new schema
   */
  def copySubsetWithRename(columnNamesWithRename: Map[String, String]): Schema = {
    val preservedOrderColumnNames = columnNames.filter(name => columnNamesWithRename.contains(name))
    copySubset(preservedOrderColumnNames).renameColumns(columnNamesWithRename)
  }

  /**
   * Union schemas together, keeping as much info as possible.
   *
   * Vertex and/or Edge schema information will be maintained for this schema only
   *
   * Column type conflicts will cause error
   */
  def union(schema: Schema): Schema = {
    // check for conflicts
    val newColumns: Seq[Column] = schema.columns.filterNot(c => {
      hasColumn(c.name) && {
        require(hasColumnWithType(c.name, c.dataType), s"columns with same name ${c.name} didn't have matching types"); true
      }
    })
    val combinedColumns = this.columns ++ newColumns
    copy(combinedColumns)
  }

  /**
   * get column datatype by column name
   *
   * @param columnName name of the column
   */
  def columnDataType(columnName: String): DataType = {
    column(columnName).dataType
  }

  /**
   * Get all of the info about a column - this is a nicer wrapper than tuples
   *
   * @param columnName the name of the column
   * @return complete column info
   */
  def column(columnName: String): Column = {
    val i = namesToIndices.getOrElse(columnName, throw new IllegalArgumentException(s"No column named $columnName choose between: $columnNamesAsString"))
    columns(i)
  }

  /**
   * Convenience method for optionally getting a Column.
   *
   * This is helpful when specifying a columnName was optional for the user.
   *
   * @param columnName the name of the column
   * @return complete column info, if a name was provided
   */
  def column(columnName: Option[String]): Option[Column] = {
    columnName match {
      case Some(name) => Some(column(name))
      case None => None
    }
  }

  /**
   * Select a subset of columns.
   *
   * List can be empty.
   */
  def columns(columnNames: Iterable[String]): Iterable[Column] = {
    columnNames.map(column)
  }

  /**
   * Validates a Map argument used for renaming schema, throwing exceptions for violations
   *
   * @param names victimName -> newName
   */
  def validateRenameMapping(names: Map[String, String], forCopy: Boolean = false): Unit = {
    if (names.isEmpty)
      throw new IllegalArgumentException(s"Empty column name map provided.  At least one name is required")
    val victimNames = names.keys.toList
    validateColumnsExist(victimNames)
    val newNames = names.values.toList
    if (newNames.size != newNames.distinct.size) {
      throw new IllegalArgumentException(s"Invalid new column names are not unique: $newNames")
    }
    if (!forCopy) {
      val safeNames = columnNamesExcept(victimNames)
      for (n <- newNames) {
        if (safeNames.contains(n)) {
          throw new IllegalArgumentException(s"Invalid new column name '$n' collides with existing names which are not being renamed: $safeNames")
        }
      }
    }
  }

  /**
   * Get all of the info about a column - this is a nicer wrapper than tuples
   *
   * @param columnIndex the index for the column
   * @return complete column info
   */
  def column(columnIndex: Int): Column = columns(columnIndex)

  /**
   * Add a column to the schema
   *
   * @param column New column
   * @return a new copy of the Schema with the column added
   */
  def addColumn(column: Column): Schema = {
    if (columnNames.contains(column.name)) {
      throw new IllegalArgumentException(s"Cannot add a duplicate column name: ${column.name}")
    }
    copy(columns = columns :+ column)
  }

  /**
   * Add a column to the schema
   *
   * @param columnName name
   * @param dataType the type for the column
   * @return a new copy of the Schema with the column added
   */
  def addColumn(columnName: String, dataType: DataType): Schema = {
    addColumn(Column(columnName, dataType))
  }

  /**
   * Add a column if it doesn't already exist.
   *
   * Throws error if column name exists with different data type
   */
  def addColumnIfNotExists(columnName: String, dataType: DataType): Schema = {
    if (hasColumn(columnName)) {
      requireColumnIsType(columnName, DataTypes.float64)
      this
    }
    else {
      addColumn(columnName, dataType)
    }
  }

  /**
   * Returns a new schema with the given columns appended.
   */
  def addColumns(newColumns: Seq[Column]): Schema = {

    copy(columns = columns ++ newColumns)
  }

  /**
   * Add column but fix the name automatically if there is any conflicts with existing column names.
   */
  def addColumnFixName(column: Column): Schema = {
    this.addColumn(this.getNewColumnName(column.name), column.dataType)
  }

  /**
   * Add columns but fix the names automatically if there are any conflicts with existing column names.
   */
  def addColumnsFixNames(columnsToAdd: Seq[Column]): Schema = {
    var schema = this
    for {
      column <- columnsToAdd
    } {
      schema = schema.addColumnFixName(column)
    }
    schema
  }

  /**
   * Remove a column from this schema
   *
   * @param columnName the name to remove
   * @return a new copy of the Schema with the column removed
   */
  def dropColumn(columnName: String): Schema = {
    copy(columns = columns.filterNot(column => column.name == columnName))
  }

  /**
   * Remove a list of columns from this schema
   *
   * @param columnNames the names to remove
   * @return a new copy of the Schema with the columns removed
   */
  def dropColumns(columnNames: Iterable[String]): Schema = {
    var newSchema = this
    if (columnNames != null) {
      columnNames.foreach(columnName => {
        newSchema = newSchema.dropColumn(columnName)
      })
    }
    newSchema
  }

  /**
   * Drop all columns with the 'ignore' data type.
   *
   * The ignore data type is a slight hack for ignoring some columns on import.
   */
  def dropIgnoreColumns(): Schema = {
    dropColumns(columns.filter(col => col.dataType.equalsDataType(DataTypes.ignore)).map(col => col.name))
  }

  /**
   * Remove columns by the indices
   *
   * @param columnIndices the indices to remove
   * @return a new copy of the Schema with the columns removed
   */
  def dropColumnsByIndex(columnIndices: Seq[Int]): Schema = {
    val remainingColumns = columns.zipWithIndex.filterNot {
      case (col, index) =>
        columnIndices.contains(index)
    }.map { case (col, index) => col }

    copy(remainingColumns)
  }

  /**
   * Convert data type for a column
   *
   * @param columnName the column to change
   * @param updatedDataType the new data type for that column
   * @return the updated Schema
   */
  def convertType(columnName: String, updatedDataType: DataType): Schema = {
    val col = column(columnName)
    val index = columnIndex(columnName)
    copy(columns = columns.updated(index, col.copy(dataType = updatedDataType)))
  }

  /**
   * Convert data type for a column
   * @param columnIndex index of the column to change
   * @param updatedDataType the new data type for that column
   * @return the updated Schema
   */
  def convertType(columnIndex: Int, updatedDataType: DataType): Schema = {
    val col = column(columnIndex)
    copy(columns = columns.updated(columnIndex, col.copy(dataType = updatedDataType)))
  }

  /**
   * Rename a column
   *
   * @param existingName the old name
   * @param newName the new name
   * @return the updated schema
   */
  def renameColumn(existingName: String, newName: String): Schema = {
    renameColumns(Map(existingName -> newName))
  }

  /**
   * Renames several columns
   *
   * @param names oldName -> newName
   * @return new renamed schema
   */
  def renameColumns(names: Map[String, String]): Schema = {
    validateRenameMapping(names)
    copy(columns = columns.map({
      case found if names.contains(found.name) => found.copy(name = names(found.name))
      case notFound => notFound.copy()
    }))
  }

  /**
   * Re-order the columns in the schema.
   *
   * No columns will be dropped.  Any column not named will be tacked onto the end.
   *
   * @param columnNames the names you want to occur first, in the order you want
   * @return the updated schema
   */
  def reorderColumns(columnNames: Vector[String]): Schema = {
    validateColumnsExist(columnNames)
    val reorderedColumns = columnNames.map(name => column(name))
    val additionalColumns = columns.filterNot(column => columnNames.contains(column.name))
    copy(columns = reorderedColumns ++ additionalColumns)
  }

  /**
   * Get the list of columns except those provided
   *
   * @param columnNamesToExclude columns you want to filter
   * @return the other columns, if any
   */
  def columnsExcept(columnNamesToExclude: Seq[String]): Seq[Column] = {
    this.columns.filter(column => !columnNamesToExclude.contains(column.name))
  }

  /**
   * Get the list of column names except those provided
   *
   * @param columnNamesToExclude column names you want to filter
   * @return the other column names, if any
   */
  def columnNamesExcept(columnNamesToExclude: Seq[String]): Seq[String] = {
    for { c <- columns if !columnNamesToExclude.contains(c.name) } yield c.name
  }

  /**
   * Convert the current schema to a FrameSchema.
   *
   * This is useful when copying a Schema whose internals might be a VertexSchema
   * or EdgeSchema but you need to make sure it is a FrameSchema.
   */
  def toFrameSchema: FrameSchema = {
    if (isInstanceOf[FrameSchema]) {
      this.asInstanceOf[FrameSchema]
    }
    else {
      new FrameSchema(columns)
    }
  }

  /**
   * create a column name that is unique, suitable for adding to the schema
   * (subject to race conditions, only provides unique name for schema as
   * currently defined)
   *
   * @param candidate a candidate string to start with, an _N number will be
   *                  append to make it unique
   * @return unique column name for this schema, as currently defined
   */
  def getNewColumnName(candidate: String): String = {
    var newName = candidate
    var i: Int = 0
    while (columnNames.contains(newName)) {
      newName = candidate + s"_$i"
      i += 1
    }
    newName
  }

}

