package org.trustedanalytics.at.interfaces

import org.apache.hadoop.io.LongWritable
import org.trustedanalytics.at.schema.{ FrameSchema, Column, DataTypes, Schema }
import reflect.runtime.universe._

import org.apache.hadoop.io.Text
import org.apache.spark.SparkContext
//import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.{ Row, DataFrame }
import org.apache.spark.sql.catalyst.expressions.{ GenericRow, GenericMutableRow }
import org.apache.spark.sql.types._
//import org.trustedanalytics.atk.domain.frame.load.LineParser
//import org.trustedanalytics.atk.domain.frame.load.LineParserArguments
//import org.trustedanalytics.atk.domain.frame.load.{ LineParser, LineParserArguments }
//import org.trustedanalytics.atk.domain.schema.Column
//import org.trustedanalytics.atk.domain.schema.Column
//import org.trustedanalytics.atk.domain.schema.DataTypes
//import org.trustedanalytics.atk.domain.schema.DataTypes
//import org.trustedanalytics.atk.domain.schema.FrameSchema
//import org.trustedanalytics.atk.domain.schema._
//import org.trustedanalytics.atk.engine.EngineConfig
//import org.trustedanalytics.atk.engine.EngineConfig
//import org.trustedanalytics.atk.engine.frame.MiscFrameFunctions
//import org.trustedanalytics.atk.engine.frame.SparkFrame
//import org.trustedanalytics.atk.engine.frame._
import org.apache.hadoop.io.LongWritable
//import org.apache.spark.frame.FrameRdd
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.hadoop.io.Text
//import org.trustedanalytics.atk.engine.frame.plugins.load.TextPlugin.CsvRowParser
//import org.trustedanalytics.atk.engine.frame.plugins.load.TextPlugin.MultiLineTaggedInputFormat
//import org.trustedanalytics.atk.engine.frame.plugins.load.TextPlugin.ParseResultRddWrapper
//import org.trustedanalytics.atk.engine.frame.plugins.load.TextPlugin.RowParseResult
//import org.trustedanalytics.atk.engine.frame.plugins.load.TextPlugin.UploadParser
//import org.trustedanalytics.atk.engine.frame.plugins.load.TextPlugin._
//import org.trustedanalytics.atk.engine.plugin.Invocation
//import org.trustedanalytics.atk.engine.plugin.Invocation

//import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

case class LineParserArguments(separator: Char, schema: Schema, skip_rows: Option[Int]) {
  skip_rows match {
    case e: Some[Int] => require(skip_rows.get >= 0, "value for skip_header_lines cannot be negative")
    case _ =>
  }
}

/**
 * Helper functions for loading an RDD
 */
object LoadRddFunctions extends Serializable {

  /**
   * Schema for Error Frames
   */
  //val ErrorFrameSchema = new FrameSchema(List(Column("original_row", DataTypes.str), Column("error_message", DataTypes.str)))

  /**
   * Load each line from CSV file into an RDD of Row objects.
   * @param sc SparkContext used for textFile reading
   * @param fileName name of file to parse
   * @param parser the parser
   * @param minPartitions minimum number of partitions for text file.
   * @param startTag Start tag for XML or JSON parsers
   * @param endTag Start tag for XML or JSON parsers
   * @param isXml True for XML input files
   * @return RDD of Row objects
   */
  def loadAndParseLines(sc: SparkContext,
                        fileName: String,
                        parser: LineParserArguments,
                        minPartitions: Option[Int] = None,
                        startTag: Option[List[String]] = None,
                        endTag: Option[List[String]] = None,
                        isXml: Boolean = false): RDD[Array[Any]] = {

    val fileContentRdd: RDD[String] = (minPartitions match {
      case Some(partitions) => sc.textFile(fileName, partitions)
      case _ => sc.textFile(fileName)
    }).filter(_.trim() != "")
    println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
    val c = fileContentRdd.count()
    println(s"fileContentRDD! $fileName, c=$c")
    println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
    val p = parse(fileContentRdd, parser)
    p
    //val r = toRowRdd(null, p)
    //r
  }

  def toRowRddWithAllStringFields(raa: RDD[Array[Any]]): Tuple2[RDD[org.apache.spark.sql.Row], Schema] = {
    var schema: Schema = null
    val rowRDD: RDD[org.apache.spark.sql.Row] = raa.map(row => {
      val rowArray = new Array[Any](row.length)
      row.zipWithIndex.map {
        case (o, i) =>
          o match {
            case null => null
            case _ =>
              val colType = DataTypes.string
              rowArray(i) = o.asInstanceOf[colType.ScalaType]
          }
      }
      if (schema == null) {
        schema = FrameSchema((for (i <- row.indices) yield Column(s"col$i", DataTypes.string)).toList)
      }
      else if (schema.columns.length != row.length) {
        throw new RuntimeException("Rows in RDD have mismatched field counts")
      }
      new GenericRow(rowArray)
    })
    (rowRDD, schema)
  }

  //    startTag match {
  //        case Some(s) =>
  //          val conf = new org.apache.hadoop.conf.Configuration()
  //          conf.setStrings(MultiLineTaggedInputFormat.START_TAG_KEY, s: _*) //Treat s as a Varargs parameter
  //        val e = endTag.get
  //          conf.setStrings(MultiLineTaggedInputFormat.END_TAG_KEY, e: _*)
  //          conf.setBoolean(MultiLineTaggedInputFormat.IS_XML_KEY, isXml)
  //          sc.newAPIHadoopFile[LongWritable, Text, MultiLineTaggedInputFormat](fileName, classOf[MultiLineTaggedInputFormat], classOf[LongWritable], classOf[Text], conf)
  //            .map(row => row._2.toString).filter(_.trim() != "")
  //        case None =>
  //          val rdd = minPartitions match {
  //            case Some(partitions) => sc.textFile(fileName, partitions)
  //            case _ => sc.textFile(fileName)
  //          }
  //          rdd.filter(_.trim() != "")
  //      }

  //    if (parser != null) {
  //
  ////      // parse a sample so we can bail early if needed
  //      parseSampleOfData(fileContentRdd, parser)

  //      // re-parse the entire file
  //      parse(fileContentRdd, parser)
  //    }
  ////    else {
  //      val listColumn = List(Column("data_lines", DataTypes.str))
  //      val rows = fileContentRdd.map(s => Row(s))
  //      ParseResultRddWrapper(new FrameRdd(new FrameSchema(listColumn), rows), null)
  //    }
  //  }

  def toRowRDD(schema: Schema, rows: RDD[Array[Any]]): RDD[org.apache.spark.sql.Row] = {
    val rowRDD: RDD[org.apache.spark.sql.Row] = rows.map(row => {
      val rowArray = new Array[Any](row.length)
      row.zipWithIndex.map {
        case (o, i) =>
          o match {
            case null => null
            case _ =>
              val colType = schema.column(i).dataType
              rowArray(i) = o.asInstanceOf[colType.ScalaType]

          }
      }
      new GenericRow(rowArray)
    })
    rowRDD
  }

  def toRowRdd(rows: RDD[_], schema: Schema): RDD[org.apache.spark.sql.Row] = {
    getRddType(rows) match {
      case x if x <:< typeOf[Array[Any]] => toRowRDD(schema, rows.asInstanceOf[RDD[Array[Any]]])
      case x if x <:< typeOf[Row] => rows.asInstanceOf[RDD[Row]]
    }
  }

  def getRddType[T: TypeTag](rdd: RDD[T]): Type = typeOf[T] match {
    case x if x <:< typeOf[Array[Any]] => typeOf[Array[Any]]
    case x if x <:< typeOf[Row] => typeOf[Row]
  }

  //  /**
  //   * Load each line from client data into an RDD of Row objects.
  //   * @param sc SparkContext used for textFile reading
  //   * @param data data to parse
  //   * @param parser parser provided
  //   * @return  RDD of Row objects
  //   */
  //  def loadAndParseData(sc: SparkContext,
  //                       data: List[List[Any]],
  //                       parser: LineParser): ParseResultRddWrapper = {
  //    val dataContentRDD: RDD[Any] = sc.parallelize(data)
  //    // parse a sample so we can bail early if needed
  //    parseSampleOfData(dataContentRDD, parser)
  //
  //    // re-parse the entire file
  //    parse(dataContentRDD, parser)
  //  }

  //  /**
  //   * Union the additionalData onto the end of the existingFrame
  //   * @param existingFrame the target DataFrame that may or may not already have data
  //   * @param additionalData the data to add to the existingFrame
  //   * @return the frame with updated schema
  //   */
  //  def unionAndSave(existingFrame: SparkFrame, additionalData: FrameRdd)(implicit invocation: Invocation): SparkFrame = {
  //    existingFrame.save(existingFrame.rdd.union(additionalData))
  //  }

  //  /**
  //   * Parse a sample of the file so we can bail early if a certain threshold fails.
  //   *
  //   * Throw an exception if too many rows can't be parsed.
  //   *
  //   * @param fileContentRdd the rows that need to be parsed (the file content)
  //   * @param parser the parser to use
  //   */
  ////  private[frame] def parseSampleOfData[T: ClassTag](fileContentRdd: RDD[T],
  //                                                    parser: LineParser): Unit = {
  //
  //    //parse the first number of lines specified as sample size and make sure the file is acceptable
  //    val sampleSize = EngineConfig.frameLoadTestSampleSize
  //    val threshold = EngineConfig.frameLoadTestFailThresholdPercentage
  //
  //    val sampleRdd = MiscFrameFunctions.getPagedRdd[T](fileContentRdd, 0, sampleSize, sampleSize)
  //
  //    //cache the RDD since it will be used multiple times
  //    sampleRdd.cache()
  //
  //    val preEvaluateResults = parse(sampleRdd, parser)
  //    val failedCount = preEvaluateResults.errorLines.count()
  //    val sampleRowsCount: Long = sampleRdd.count()
  //
  //    val failedRatio: Long = if (sampleRowsCount == 0) 0 else 100 * failedCount / sampleRowsCount
  //
  //    //don't need it anymore
  //    sampleRdd.unpersist()
  //
  //    if (failedRatio >= threshold) {
  //      val errorExampleRecord = preEvaluateResults.errorLines.first().copy()
  //      val errorRow = errorExampleRecord { 0 }
  //      val errorMessage = errorExampleRecord { 1 }
  //      throw new Exception(s"Parse failed on $failedCount rows out of the first $sampleRowsCount, " +
  //        s" please ensure your schema is correct.\nExample record that parser failed on : $errorRow    " +
  //        s" \n$errorMessage")
  //    }
  //  }

  /**
   * Parse rows and separate into successes and failures
   * @param rowsToParse the rows that need to be parsed (the file content)
   * @param parser the parser to use
   * @return the parse result - successes and failures
   */
  private def parse(rowsToParse: RDD[String], parser: LineParserArguments): RDD[Array[Any]] = {

    val schemaArgs = parser.schema
    val skipRows = parser.skip_rows
    val parserFunction = getLineParser(parser) //, schemaArgs.columns.map(_._2).toArray)

    val parseResultRdd: RDD[Array[Any]] = rowsToParse.mapPartitionsWithIndex {
      case (partition, lines) =>
        if (partition == 0) {
          lines.drop(skipRows.getOrElse(0)).map(parserFunction)
        }
        else {
          lines.map(parserFunction)
        }
    }
    //    try {
    //      parseResultRdd.cache()
    //      val successesRdd = parseResultRdd//.filter(rowParseResult => rowParseResult.parseSuccess)
    //        .map(rowParseResult => rowParseResult.row)
    //      val failuresRdd = parseResultRdd//.filter(rowParseResult => !rowParseResult.parseSuccess)
    //        .map(rowParseResult => rowParseResult.row)
    //
    //      val schema = parser.arguments.schema
    //      new ParseResultRddWrapper(FrameRdd.toFrameRdd(schema.schema, successesRdd), FrameRdd.toFrameRdd(ErrorFrameSchema, failuresRdd))
    //    }
    //    finally {
    //      parseResultRdd.unpersist(blocking = false)
    //    }
    parseResultRdd
  }

  private def getLineParser[T](parser: LineParserArguments): T => Array[Any] = { //Row, columnTypes: Array[DataTypes.DataType]): T => RowParseResult = {
    //parser.name match {
    //TODO: look functions up in a table rather than switching on names
    //      case "builtin/line/separator" =>
    //        val args = parser.arguments match {
    //          //TODO: genericize this argument conversion
    //          case a: LineParserArguments => a
    //          case x => throw new IllegalArgumentException(
    //            "Could not convert instance of " + x.getClass.getName + " to  arguments for builtin/line/separator")
    //        }
    val rowParser = new CsvRowParser(parser.separator) //, columnTypes)
    s => rowParser(s.asInstanceOf[String]).asInstanceOf[Array[Any]]
    //      case "builtin/upload" =>
    //        val uploadParser = new UploadParser(columnTypes)
    //        row => uploadParser(row.asInstanceOf[List[Any]])
    //case x => throw new Exception("Unsupported parser: " + x)
    //}
  }

}
import org.apache.commons.csv.{ CSVFormat, CSVParser }
//import org.trustedanalytics.atk.domain.schema.DataTypes
//import org.trustedanalytics.atk.domain.schema.DataTypes.DataType

import scala.collection.JavaConversions.asScalaIterator

/**
 * Split a string based on delimiter into List[String]
 * <p>
 * Usage:
 * scala> import com.trustedanalytics.engine.Row
 * scala> val out = RowParser.apply("foo,bar")
 * scala> val out = RowParser.apply("a,b,\"foo,is this ,bar\",foobar ")
 * </p>
 *
 * @param separator delimiter character
 */
class CsvRowParser(separator: Char) extends Serializable { //}, columnTypes: Array[DataType]) extends Serializable {

  val csvFormat = CSVFormat.RFC4180.withDelimiter(separator)

  //val converter = DataTypes.parseMany(columnTypes)(_)

  /**
   * Parse a line into a RowParseResult
   * @param line a single line
   * @return the result - either a success row or an error row
   */
  def apply(line: String): Array[String] = {
    //try {
    val parts = splitLineIntoParts(line)
    parts.asInstanceOf[Array[String]]
    //}
    //catch {
    //case e: Exception =>
    //RowParseResult(parseSuccess = false, Array(line, e.toString))
    //}
  }

  private def splitLineIntoParts(line: String): Array[String] = {
    val records = CSVParser.parse(line, csvFormat).getRecords
    if (records.isEmpty) {
      Array.empty[String]
    }
    else {
      records.get(0).iterator().toArray
    }

  }

}
