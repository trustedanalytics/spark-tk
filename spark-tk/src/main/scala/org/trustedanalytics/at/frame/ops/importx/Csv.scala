package org.trustedanalytics.at.frame.ops.importx

// todo: bring back skip_rows
// todo: bring back schema validation
// todo: bring back append and schema merge

import org.apache.commons.csv.{ CSVFormat, CSVParser }
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.trustedanalytics.at.frame.rdd.PythonJavaRdd
import org.trustedanalytics.at.frame.schema.Schema

import scala.collection.JavaConversions.asScalaIterator

/**
 * Helper functions for CSV import/export
 */
object Csv extends Serializable {

  /**
   * Load each line from CSV file into an RDD of Row objects.
   * @param sc SparkContext used for textFile reading
   * @param fileName name of file to parse
   * @param delimiter the delimiter character
   * @param minPartitions minimum number of partitions for text file.
   * @return RDD of Row objects
   */
  def importCsvFile(sc: SparkContext,
                    fileName: String,
                    schema: Schema,
                    delimiter: Char,
                    minPartitions: Option[Int] = None): RDD[Row] = {

    val fileContentRdd: RDD[String] = (minPartitions match {
      case Some(partitions) => sc.textFile(fileName, partitions)
      case _ => sc.textFile(fileName)
    }).filter(_.trim() != "")
    val raa = parse(fileContentRdd, delimiter)
    PythonJavaRdd.toRowRdd(raa, schema)
  }

  private def parse(rowsToParse: RDD[String], delimiter: Char): RDD[Array[Any]] = {
    val parserFunction = getLineParser(delimiter)
    rowsToParse.map(parserFunction)
  }

  private def getLineParser[T](delimiter: Char): T => Array[Any] = {
    val rowParser = new CsvRowParser(delimiter)
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
 * @param delimiter delimiter character
 */
class CsvRowParser(delimiter: Char) extends Serializable {

  val csvFormat = CSVFormat.RFC4180.withDelimiter(delimiter)

  def apply(line: String): Array[String] = {
    val parts = splitLineIntoParts(line)
    parts.asInstanceOf[Array[String]]
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

