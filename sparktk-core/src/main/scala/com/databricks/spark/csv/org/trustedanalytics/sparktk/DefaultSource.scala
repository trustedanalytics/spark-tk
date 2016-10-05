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
package com.databricks.spark.csv.org.trustedanalytics.sparktk

// So there are just two classes in here, DefaultSource and TkCsvRelation
// both taken from com.data.bricks.spark.csv with only minor edits.

// This was done because many things were marked private preventing
// natural inheritance changes.  The edits change the parser to replace
// values which cause parse errors with null, rather than rejecting the
// entire row or raising an exception.  Look for **sparktk addition comments
// to see the very few areas where this code differs from databricks

// Strategy:  Spark's parser framework ends up looking for a class
// called DefaultSource and loads it dynamically, so herein is the
// copied DefaultSource for the CSV Parser.  This DefaultSource is
// responsible for creating a Relation class.  Herein, is a copied
// CsvRelation class named TkCsvRelation which has the few code
// changes to handle the parse errors

// See pull request https://github.com/databricks/spark-csv/pull/2980
// Apparently it won't get merged because the parser has since been
// ported to Spark 2.0.  The strategy in this file will need to be
// readdressed when sparkt-tk moves to Spark 2.0.

import java.io.IOException
import java.text.{ ParseException, SimpleDateFormat }

import com.databricks.spark.csv.readers.{ BulkCsvReader, LineCsvReader }
import com.databricks.spark.csv.util._
import com.databricks.spark.csv.{ CsvRelation, defaultCsvFormat }
import org.apache.commons.csv.{ CSVFormat, CSVParser }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ SaveMode, DataFrame, Row, SQLContext }

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{ StructType, StringType, StructField }
import org.slf4j.LoggerFactory
import org.trustedanalytics.sparktk.frame.internal.ops.exportdata.ExportToCsv // Added for 1.5 compatibility, move to 1.6 should correct

import scala.util.control.NonFatal
import scala.collection.JavaConversions._

class DefaultSource
    extends RelationProvider
    with SchemaRelationProvider
    with CreatableRelationProvider {

  private def checkPath(parameters: Map[String, String]): String = {
    parameters.getOrElse("path", sys.error("'path' must be specified for CSV data."))
  }

  /**
   * Creates a new relation for data store in CSV given parameters.
   * Parameters have to include 'path' and optionally 'delimiter', 'quote', and 'header'
   */
  override def createRelation(
    sqlContext: SQLContext,
    parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext, parameters, null)
  }

  /**
   * Creates a new relation for data store in CSV given parameters and user supported schema.
   * Parameters have to include 'path' and optionally 'delimiter', 'quote', and 'header'
   */
  override def createRelation(
    sqlContext: SQLContext,
    parameters: Map[String, String],
    schema: StructType): BaseRelation = { // **sparktk change to return BaseRelation instead of CsvRelation**
    val path = checkPath(parameters)
    val delimiter = TypeCast.toChar(parameters.getOrElse("delimiter", ","))

    val quote = parameters.getOrElse("quote", "\"")
    val quoteChar: Character = if (quote == null) {
      null
    }
    else if (quote.length == 1) {
      quote.charAt(0)
    }
    else {
      throw new Exception("Quotation cannot be more than one character.")
    }

    val escape = parameters.getOrElse("escape", null)
    val escapeChar: Character = if (escape == null) {
      null
    }
    else if (escape.length == 1) {
      escape.charAt(0)
    }
    else {
      throw new Exception("Escape character cannot be more than one character.")
    }

    val comment = parameters.getOrElse("comment", "#")
    val commentChar: Character = if (comment == null) {
      null
    }
    else if (comment.length == 1) {
      comment.charAt(0)
    }
    else {
      throw new Exception("Comment marker cannot be more than one character.")
    }

    val parseMode = parameters.getOrElse("mode", "PERMISSIVE")

    val useHeader = parameters.getOrElse("header", "false")
    val headerFlag = if (useHeader == "true") {
      true
    }
    else if (useHeader == "false") {
      false
    }
    else {
      throw new Exception("Header flag can be true or false")
    }

    val parserLib = parameters.getOrElse("parserLib", ParserLibs.DEFAULT)
    val ignoreLeadingWhiteSpace = parameters.getOrElse("ignoreLeadingWhiteSpace", "false")
    val ignoreLeadingWhiteSpaceFlag = if (ignoreLeadingWhiteSpace == "false") {
      false
    }
    else if (ignoreLeadingWhiteSpace == "true") {
      if (!ParserLibs.isUnivocityLib(parserLib)) {
        throw new Exception("Ignore whitesspace supported for Univocity parser only")
      }
      true
    }
    else {
      throw new Exception("Ignore white space flag can be true or false")
    }
    val ignoreTrailingWhiteSpace = parameters.getOrElse("ignoreTrailingWhiteSpace", "false")
    val ignoreTrailingWhiteSpaceFlag = if (ignoreTrailingWhiteSpace == "false") {
      false
    }
    else if (ignoreTrailingWhiteSpace == "true") {
      if (!ParserLibs.isUnivocityLib(parserLib)) {
        throw new Exception("Ignore whitespace supported for the Univocity parser only")
      }
      true
    }
    else {
      throw new Exception("Ignore white space flag can be true or false")
    }
    val treatEmptyValuesAsNulls = parameters.getOrElse("treatEmptyValuesAsNulls", "false")
    val treatEmptyValuesAsNullsFlag = if (treatEmptyValuesAsNulls == "false") {
      false
    }
    else if (treatEmptyValuesAsNulls == "true") {
      true
    }
    else {
      throw new Exception("Treat empty values as null flag can be true or false")
    }

    val charset = parameters.getOrElse("charset", TextFile.DEFAULT_CHARSET.name())
    // TODO validate charset?

    val inferSchema = parameters.getOrElse("inferSchema", "false")
    val inferSchemaFlag = if (inferSchema == "false") {
      false
    }
    else if (inferSchema == "true") {
      true
    }
    else {
      throw new Exception("Infer schema flag can be true or false")
    }
    val nullValue = parameters.getOrElse("nullValue", "")

    val dateFormat = parameters.getOrElse("dateFormat", null)

    val codec = parameters.getOrElse("codec", null)

    val r = CsvRelation(
      () => TextFile.withCharset(sqlContext.sparkContext, path, charset),
      Some(path),
      headerFlag,
      delimiter,
      quoteChar,
      escapeChar,
      commentChar,
      parseMode,
      parserLib,
      ignoreLeadingWhiteSpaceFlag,
      ignoreTrailingWhiteSpaceFlag,
      treatEmptyValuesAsNullsFlag,
      schema,
      inferSchemaFlag,
      codec,
      nullValue,
      dateFormat)(sqlContext)

    // **sparktk addition here**
    TkCsvRelation(r.baseRDD,
      r.location,
      r.useHeader,
      r.delimiter,
      r.quote,
      r.escape,
      r.comment,
      r.parseMode,
      r.parserLib,
      r.ignoreLeadingWhiteSpace,
      r.ignoreTrailingWhiteSpace,
      r.treatEmptyValuesAsNulls,
      r.userSchema,
      r.inferCsvSchema,
      r.codec,
      r.nullValue,
      r.dateFormat)(sqlContext)
  }

  override def createRelation(
    sqlContext: SQLContext,
    mode: SaveMode,
    parameters: Map[String, String],
    data: DataFrame): BaseRelation = {
    val path = checkPath(parameters)
    val filesystemPath = new Path(path)
    val fs = filesystemPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
    val doSave = if (fs.exists(filesystemPath)) {
      mode match {
        case SaveMode.Append =>
          sys.error(s"Append mode is not supported by ${this.getClass.getCanonicalName}")
        case SaveMode.Overwrite =>
          fs.delete(filesystemPath, true)
          true
        case SaveMode.ErrorIfExists =>
          sys.error(s"path $path already exists.")
        case SaveMode.Ignore => false
      }
    }
    else {
      true
    }
    if (doSave) {
      // Only save data when the save mode is not ignore.
      val codecClass = CompressionCodecs.getCodecClass(parameters.getOrElse("codec", null))
      // sparktk edit, for spark 1.5, this may not even work  (replace saveAsCsvFile with our own ExportToCsv)
      //data.saveAsCsvFile(path, parameters, codecClass)
      ExportToCsv.exportToCsvFile(data.rdd, filesystemPath.toString, ',')

    }

    createRelation(sqlContext, parameters, data.schema)
  }
}

// **sparktk addition --copied the CsvRelation code wholesale and make a change around to calls to "toCast"
case class TkCsvRelation protected[spark] (
  baseRDD: () => RDD[String],
  location: Option[String],
  useHeader: Boolean,
  delimiter: Char,
  quote: Character,
  escape: Character,
  comment: Character,
  parseMode: String,
  parserLib: String,
  ignoreLeadingWhiteSpace: Boolean,
  ignoreTrailingWhiteSpace: Boolean,
  treatEmptyValuesAsNulls: Boolean,
  userSchema: StructType = null,
  inferCsvSchema: Boolean,
  codec: String = null,
  nullValue: String = "",
  dateFormat: String = null)(@transient val sqlContext: SQLContext)
    extends BaseRelation with TableScan with PrunedScan with InsertableRelation {

  // Share date format object as it is expensive to parse date pattern.
  private val dateFormatter = if (dateFormat != null) new SimpleDateFormat(dateFormat) else null

  private val logger = LoggerFactory.getLogger(CsvRelation.getClass)

  // Parse mode flags
  if (!ParseModes.isValidMode(parseMode)) {
    logger.warn(s"$parseMode is not a valid parse mode. Using ${ParseModes.DEFAULT}.")
  }

  if ((ignoreLeadingWhiteSpace || ignoreLeadingWhiteSpace) && ParserLibs.isCommonsLib(parserLib)) {
    logger.warn(s"Ignore white space options may not work with Commons parserLib option")
  }

  private val failFast = ParseModes.isFailFastMode(parseMode)
  private val dropMalformed = ParseModes.isDropMalformedMode(parseMode)
  private val permissive = ParseModes.isPermissiveMode(parseMode)

  override val schema: StructType = inferSchema()

  private def tokenRdd(header: Array[String]): RDD[Array[String]] = {

    if (ParserLibs.isUnivocityLib(parserLib)) {
      univocityParseCSV(baseRDD(), header)
    }
    else {
      val csvFormat = defaultCsvFormat
        .withDelimiter(delimiter)
        .withQuote(quote)
        .withEscape(escape)
        .withSkipHeaderRecord(false)
        .withHeader(header: _*)
        .withCommentMarker(comment)

      // If header is set, make sure firstLine is materialized before sending to executors.
      val filterLine = if (useHeader) firstLine else null

      baseRDD().mapPartitions { iter =>
        // When using header, any input line that equals firstLine is assumed to be header
        val csvIter = if (useHeader) {
          iter.filter(_ != filterLine)
        }
        else {
          iter
        }
        parseCSV(csvIter, csvFormat)
      }
    }
  }

  override def buildScan: RDD[Row] = {
    val simpleDateFormatter = dateFormatter
    val schemaFields = schema.fields
    val rowArray = new Array[Any](schemaFields.length)
    tokenRdd(schemaFields.map(_.name)).flatMap { tokens =>

      if (dropMalformed && schemaFields.length != tokens.length) {
        logger.warn(s"Dropping malformed line: ${tokens.mkString(",")}")
        None
      }
      else if (failFast && schemaFields.length != tokens.length) {
        throw new RuntimeException(s"Malformed line in FAILFAST mode: ${tokens.mkString(",")}")
      }
      else {
        var index: Int = 0
        try {
          index = 0
          while (index < schemaFields.length) {
            val field = schemaFields(index)
            // **sparktk addition - catch a ParseException or NumberFormatException and fill with null
            val item = try {
              TypeCast.castTo(tokens(index), field.dataType, field.nullable,
                treatEmptyValuesAsNulls, nullValue, simpleDateFormatter)
            }
            catch {
              case e @ (_: ParseException | _: NumberFormatException) => null
            }
            rowArray(index) = item
            index = index + 1
          }
          Some(Row.fromSeq(rowArray))
        }
        catch {
          case aiob: ArrayIndexOutOfBoundsException if permissive =>
            (index until schemaFields.length).foreach(ind => rowArray(ind) = null)
            Some(Row.fromSeq(rowArray))
          case _: java.lang.NumberFormatException |
            _: IllegalArgumentException if dropMalformed =>
            logger.warn("Number format exception. " +
              s"Dropping malformed line: ${tokens.mkString(delimiter.toString)}")
            None
          case pe: java.text.ParseException if dropMalformed =>
            logger.warn("Parse exception. " +
              s"Dropping malformed line: ${tokens.mkString(delimiter.toString)}")
            None
        }
      }
    }
  }

  /**
   * This supports to eliminate unneeded columns before producing an RDD
   * containing all of its tuples as Row objects. This reads all the tokens of each line
   * and then drop unneeded tokens without casting and type-checking by mapping
   * both the indices produced by `requiredColumns` and the ones of tokens.
   */
  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    val simpleDateFormatter = dateFormatter
    val schemaFields = schema.fields
    val requiredFields = StructType(requiredColumns.map(schema(_))).fields
    val shouldTableScan = schemaFields.deep == requiredFields.deep
    val safeRequiredFields = if (dropMalformed) {
      // If `dropMalformed` is enabled, then it needs to parse all the values
      // so that we can decide which row is malformed.
      requiredFields ++ schemaFields.filterNot(requiredFields.contains(_))
    }
    else {
      requiredFields
    }
    val rowArray = new Array[Any](safeRequiredFields.length)
    if (shouldTableScan) {
      buildScan()
    }
    else {
      val safeRequiredIndices = new Array[Int](safeRequiredFields.length)
      schemaFields.zipWithIndex.filter {
        case (field, _) => safeRequiredFields.contains(field)
      }.foreach {
        case (field, index) => safeRequiredIndices(safeRequiredFields.indexOf(field)) = index
      }
      val requiredSize = requiredFields.length
      tokenRdd(schemaFields.map(_.name)).flatMap { tokens =>

        if (dropMalformed && schemaFields.length != tokens.length) {
          logger.warn(s"Dropping malformed line: ${tokens.mkString(delimiter.toString)}")
          None
        }
        else if (failFast && schemaFields.length != tokens.length) {
          throw new RuntimeException(s"Malformed line in FAILFAST mode: " +
            s"${tokens.mkString(delimiter.toString)}")
        }
        else {
          val indexSafeTokens = if (permissive && schemaFields.length > tokens.length) {
            tokens ++ new Array[String](schemaFields.length - tokens.length)
          }
          else if (permissive && schemaFields.length < tokens.length) {
            tokens.take(schemaFields.length)
          }
          else {
            tokens
          }
          try {
            var index: Int = 0
            var subIndex: Int = 0
            while (subIndex < safeRequiredIndices.length) {
              index = safeRequiredIndices(subIndex)
              val field = schemaFields(index)
              // **sparktk addition - catch a ParseException and fill with null
              val item = try {
                TypeCast.castTo(
                  indexSafeTokens(index),
                  field.dataType,
                  field.nullable,
                  treatEmptyValuesAsNulls,
                  nullValue,
                  simpleDateFormatter
                )
              }
              catch {
                case e: ParseException => null
              }
              rowArray(subIndex) = item
              subIndex = subIndex + 1
            }
            Some(Row.fromSeq(rowArray.take(requiredSize)))
          }
          catch {
            case _: java.lang.NumberFormatException |
              _: IllegalArgumentException if dropMalformed =>
              logger.warn("Number format exception. " +
                s"Dropping malformed line: ${tokens.mkString(delimiter.toString)}")
              None
            case pe: java.text.ParseException if dropMalformed =>
              logger.warn("Parse exception. " +
                s"Dropping malformed line: ${tokens.mkString(delimiter.toString)}")
              None
          }
        }
      }
    }
  }

  private def inferSchema(): StructType = {
    if (this.userSchema != null) {
      userSchema
    }
    else {
      val firstRow = if (ParserLibs.isUnivocityLib(parserLib)) {
        val escapeVal = if (escape == null) '\\' else escape.charValue()
        val commentChar: Char = if (comment == null) '\0' else comment
        val quoteChar: Char = if (quote == null) '\0' else quote
        new LineCsvReader(
          fieldSep = delimiter,
          quote = quoteChar,
          escape = escapeVal,
          commentMarker = commentChar).parseLine(firstLine)
      }
      else {
        val csvFormat = defaultCsvFormat
          .withDelimiter(delimiter)
          .withQuote(quote)
          .withEscape(escape)
          .withSkipHeaderRecord(false)
        CSVParser.parse(firstLine, csvFormat).getRecords.head.toArray
      }
      val header = if (useHeader) {
        firstRow
      }
      else {
        firstRow.zipWithIndex.map { case (value, index) => s"C$index" }
      }
      if (this.inferCsvSchema) {
        val simpleDateFormatter = dateFormatter
        InferSchema(tokenRdd(header), header, nullValue, simpleDateFormatter)
      }
      else {
        // By default fields are assumed to be StringType
        val schemaFields = header.map { fieldName =>
          StructField(fieldName.toString, StringType, nullable = true)
        }
        StructType(schemaFields)
      }
    }
  }

  /**
   * Returns the first line of the first non-empty file in path
   */
  private lazy val firstLine = {
    if (comment != null) {
      baseRDD().filter { line =>
        line.trim.nonEmpty && !line.startsWith(comment.toString)
      }.first()
    }
    else {
      baseRDD().filter { line =>
        line.trim.nonEmpty
      }.first()
    }
  }

  private def univocityParseCSV(
    file: RDD[String],
    header: Seq[String]): RDD[Array[String]] = {
    // If header is set, make sure firstLine is materialized before sending to executors.
    val filterLine = if (useHeader) firstLine else null
    val dataLines = if (useHeader) file.filter(_ != filterLine) else file
    val rows = dataLines.mapPartitionsWithIndex({
      case (split, iter) => {
        val escapeVal = if (escape == null) '\\' else escape.charValue()
        val commentChar: Char = if (comment == null) '\0' else comment
        val quoteChar: Char = if (quote == null) '\0' else quote

        new BulkCsvReader(iter, split,
          headers = header, fieldSep = delimiter,
          quote = quoteChar, escape = escapeVal, commentMarker = commentChar)
      }
    }, true)

    rows
  }

  private def parseCSV(
    iter: Iterator[String],
    csvFormat: CSVFormat): Iterator[Array[String]] = {
    iter.flatMap { line =>
      try {
        val records = CSVParser.parse(line, csvFormat).getRecords
        if (records.isEmpty) {
          logger.warn(s"Ignoring empty line: $line")
          None
        }
        else {
          Some(records.head.toArray)
        }
      }
      catch {
        case NonFatal(e) if !failFast =>
          logger.error(s"Exception while parsing line: $line. ", e)
          None
      }
    }
  }

  // The function below was borrowed from JSONRelation
  override def insert(data: DataFrame, overwrite: Boolean): Unit = {

    val filesystemPath = location match {
      case Some(p) => new Path(p)
      case None =>
        throw new IOException(s"Cannot INSERT into table with no path defined")
    }

    val fs = filesystemPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)

    if (overwrite) {
      try {
        fs.delete(filesystemPath, true)
      }
      catch {
        case e: IOException =>
          throw new IOException(
            s"Unable to clear output directory ${filesystemPath.toString} prior"
              + s" to INSERT OVERWRITE a CSV table:\n${e.toString}")
      }
      // Write the data. We assume that schema isn't changed, and we won't update it.

      val codecClass = CompressionCodecs.getCodecClass(codec)
      // sparktk edit, for spark 1.5, this may not even work  (replace saveAsCsvFile with our own ExportToCsv)
      //data.saveAsCsvFile(filesystemPath.toString, Map("delimiter" -> delimiter.toString), codecClass)
      ExportToCsv.exportToCsvFile(data.rdd, filesystemPath.toString, delimiter)
    }
    else {
      sys.error("CSV tables only support INSERT OVERWRITE for now.")
    }
  }
}
