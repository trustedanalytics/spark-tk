package org.trustedanalytics.at.file

import org.apache.commons.csv.{ CSVFormat, CSVParser, CSVPrinter }
import org.apache.commons.lang.NotImplementedException
import org.trustedanalytics.at.schema.Schema
import org.trustedanalytics.at.jconvert.PythonConvert.toRowRdd
import scala.collection.JavaConversions.asScalaIterator
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

case class LineParserArguments(separator: Char, schema: Schema, skip_rows: Option[Int]) {
  skip_rows match {
    //case e: Some[Int] => require(skip_rows.get >= 0, "value for skip_header_lines cannot be negative")
    case e: Some[Int] => throw new NotImplementedException("skip header is not implemented in this version")
    case _ =>
  }
}

/**
 * Helper functions for CSV import/export
 */
object Csv extends Serializable {

  /**
   * Load each line from CSV file into an RDD of Row objects.
   * @param sc SparkContext used for textFile reading
   * @param fileName name of file to parse
   * @param parser the parser
   * @param minPartitions minimum number of partitions for text file.
   * @return RDD of Row objects
   */
  def importCsvFile(sc: SparkContext,
                    fileName: String,
                    parser: LineParserArguments,
                    minPartitions: Option[Int] = None): RDD[Row] = {

    val fileContentRdd: RDD[String] = (minPartitions match {
      case Some(partitions) => sc.textFile(fileName, partitions)
      case _ => sc.textFile(fileName)
    }).filter(_.trim() != "")
    val raa = parse(fileContentRdd, parser)
    toRowRdd(raa, parser.schema)
  }

  def exportCsvFile(
    rdd: RDD[Row],
    filename: String,
    separator: Char) = {

    val csvFormat = CSVFormat.RFC4180.withDelimiter(separator)

    val csvRdd = rdd.map(row => {
      val stringBuilder = new java.lang.StringBuilder
      val printer = new CSVPrinter(stringBuilder, csvFormat)
      val array = row.toSeq.map {
        case null => ""
        case arr: ArrayBuffer[_] => arr.mkString(",")
        case seq: Seq[_] => seq.mkString(",")
        case x => x.toString
      }
      for (i <- array) printer.print(i)
      stringBuilder.toString
    })
    csvRdd.saveAsTextFile(filename)
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

  /**
   * Parse rows and separate into successes and failures
   * @param rowsToParse the rows that need to be parsed (the file content)
   * @param parserArgs the parser to use
   * @return the parse result - successes and failures
   */
  private def parse(rowsToParse: RDD[String], parserArgs: LineParserArguments): RDD[Array[Any]] = {
    //val schemaArgs = parser.schema
    //val skipRows = parser.skip_rows
    val parserFunction = getLineParser(parserArgs) //, schemaArgs.columns.map(_._2).toArray)
    rowsToParse.map(parserFunction)
  }

  private def getLineParser[T](parserArgs: LineParserArguments): T => Array[Any] = { //Row, columnTypes: Array[DataTypes.DataType]): T => RowParseResult = {
    val rowParser = new CsvRowParser(parserArgs.separator) //, columnTypes)
    s => rowParser(s.asInstanceOf[String]).asInstanceOf[Array[Any]]
  }

}

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
//  def toRowRDD(schema: Schema, rows: RDD[Array[Any]]): RDD[org.apache.spark.sql.Row] = {
//    val rowRDD: RDD[org.apache.spark.sql.Row] = rows.map(row => {
//      val rowArray = new Array[Any](row.length)
//      row.zipWithIndex.map {
//        case (o, i) =>
//          o match {
//            case null => null
//            case _ =>
//              val colType = schema.column(i).dataType
//              try {
//                rowArray(i) = o.asInstanceOf[colType.ScalaType]
//              }
//              catch {
//                case e: Exception => null
//              }
//          }
//      }
//      new GenericRow(rowArray)
//    })
//    rowRDD
//  }
//
//  def toAnyRDD(schema: Schema, rows: RDD[Row]): RDD[Array[Any]] = {
//    val anyRDD: RDD[Array[Any]] = rows.map(row => {
//      val rowArray = new Array[Any](row.length)
//      row.toSeq.zipWithIndex.map {
//        case (o, i) =>
//          o match {
//            case null => null
//            case _ =>
//              val colType = schema.column(i).dataType
//              try {
//                rowArray(i) = o.asInstanceOf[colType.ScalaType]
//              }
//              catch {
//                case e: Exception => null
//              }
//          }
//      }.toArray
//    })
//    anyRDD
//  }
//  def toRowRddWithAllStringFields(raa: RDD[Array[Any]]): Tuple2[RDD[org.apache.spark.sql.Row], Schema] = {
//    var schema: Schema = null
//    println("Hi")
//    val rowRDD: RDD[org.apache.spark.sql.Row] = raa.map(row => {
//      val rowArray = new Array[Any](row.length)
//      row.zipWithIndex.map {
//        case (o, i) =>
//          o match {
//            case null => null
//            case _ =>
//              print("here first")
//              val colType = DataTypes.string
//              println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
//              println(s"colType=$colType")
//              println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
//              rowArray(i) = o.asInstanceOf[colType.ScalaType]
//          }
//      }
//      if (schema == null) {
//        schema = FrameSchema((for (i <- row.indices) yield Column(s"col$i", DataTypes.string)).toList)
//      }
//      else if (schema.columns.length != row.length) {
//        throw new RuntimeException("Rows in RDD have mismatched field counts")
//      }
//      new GenericRow(rowArray)
//    })
//    (rowRDD, schema)
//  }

