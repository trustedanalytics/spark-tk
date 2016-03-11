package org.trustedanalytics.at.interfaces

import java.util
import java.util.ArrayList

import net.razorvine.pickle.Pickler
import org.apache.spark.SparkContext
import org.apache.spark.api.java.{ JavaRDD, JavaSparkContext }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.trustedanalytics.at.file.{ LineParserArguments, Csv }
import org.trustedanalytics.at.frame.{ TakeTrait, BinColumnTrait }
import org.trustedanalytics.at.schema.{ FrameSchema, Schema }
import org.trustedanalytics.at.serial.PythonSerialization

import scala.collection.mutable

import java.util.{ ArrayList => JArrayList }

//import org.apache.spark.SparkContext
//import org.apache.spark.sql.Row
//import org.apache.spark.sql.types.DataType

//--------------------------------------------------------------------------------------

//case class Column(name: String, dataType: DataType)
//case class Column(name: String, dataType: String)
//case class Schema(columns: List[Column])

//--------------------------------------------------------------------------------------

case class ImmutableFrame(rdd: RDD[Row], schema: Schema)
//case class ImmutableFrame(rdd: RDD[Array[Any]], schema: Schema)

//--------------------------------------------------------------------------------------

class TK(jsc: JavaSparkContext) extends Serializable {

  private val sc = jsc.sc
  private val a = Array[Int](10, 11, 12, 13)

  def createEmptyFrame(): Frame = {
    //new Frame(sc.emptyRDD: RDD[Row], null)
    val schema = FrameSchema()
    new Frame(PythonSerialization.toRowRdd(sc.emptyRDD: RDD[Array[Any]], schema), schema)
  }

  def frameSchemaToScala(pythonSchema: JArrayList[JArrayList[String]]): Schema = {
    PythonSerialization.frameSchemaToScala(pythonSchema)
  }

  def getArray(): Array[Int] = a

  //def xcount(jrdd: JavaRDD[Row]): Long = jrdd.count()

  //def xtake(jrdd: JavaRDD[Row]): String = jrdd.rdd.map(row => s"$row").collect().mkString("|")

  def sayHello(): String = "Hello from TK"

}

//--------------------------------------------------------------------------------------

//--------------------------------------------------------------------------------------

class Frame(r: RDD[Row], s: Schema) extends BaseFrame // named "r" and "s" because naming them "rdd" and "schema" masks the base members "rdd" and "schema" in this scope
    //class Frame(rdd: RDD[Array[Any]], schema: Schema) extends BaseFrame
    with AddColumnsTrait
    with AppendCsvFile
    with BinColumnTrait
    with CountTrait
    with ExportToCsvTrait
    with Save
    with TakeTrait {
  init(r, s)

  /**
   * (typically called from pyspark, with jrdd)
   * @param jrdd
   * @param schema
   */
  //def this(rdd: JavaRDD[Row], schema: Schema) = {
  def this(jrdd: JavaRDD[Array[Any]], schema: Schema) = {
    this(PythonSerialization.toRowRdd(jrdd.rdd, schema), schema)
  }

  //  def this(jrdd: JavaRDD[Array[Any]], pythonStringSchema: util.ArrayList[util.ArrayList[String]]) = {
  //    this(jrdd, PythonSerialization.frameSchemaToScala(pythonStringSchema))
  //  }

  //  def take(n: Int): scala.Array[Row] = {
  //    rdd.saveAsTextFile("/home/blbarker/tmp/scala_take")
  //    rdd.take(n)
  //  }

  //  def takeForPython(n: Int): scala.Array[Seq[Any]] = {
  //    take(n).map { row => row.toSeq }
  //  }
}

//class Frome(rdd: JavaRDD[Array[Any]]) {
//  def see(): String = "I see you"
//}

trait Save {}
//--------------------------------------------------------------------------------------

trait AppendCsvFile extends BaseFrame {

  // This isn't necessary to enable ATK, as it will all happen with pyspark.

  /**
   * Add those columns
   */
  //def appendCsvFile(fileName: String, separator: Char): RDD[Row] = { //func: Array[Any] => Seq[Any], schema: Schema): Unit = {
  def appendCsvFile(fileName: String, parserArgs: LineParserArguments): RDD[Row] = { //Array[Any]] = { //func: Array[Any] => Seq[Any], schema: Schema): Unit = {
    execute(AppendCsvFileTransform(rdd.context, fileName, parserArgs))
  }
}

case class AppendCsvFileTransform(sc: SparkContext, fileName: String, parserArgs: LineParserArguments) extends FrameTransform { //} func: Array[Any] => Seq[Any], schema: Schema) extends FrameTransform {

  override def work(immutableFrame: ImmutableFrame): ImmutableFrame = {
    val rdd = Csv.importCsvFile(sc, fileName, parserArgs)
    ImmutableFrame(rdd, parserArgs.schema)
  }

}

//--------------------------------------------------------------------------------------

trait ExportToCsvTrait extends BaseFrame {

  // This isn't necessary to enable ATK, as it will all happen with pyspark.

  /**
   * Add those columns
   */
  //def appendCsvFile(fileName: String, separator: Char): RDD[Row] = { //func: Array[Any] => Seq[Any], schema: Schema): Unit = {
  def exportToCsv(fileName: String) = {
    execute(ExportToCsv(rdd.context, fileName))
  }
}

case class ExportToCsv(sc: SparkContext, fileName: String) extends FrameSummarization[Unit] { //}, schema: Schema, lineParserArguments: LineParserArguments) extends FrameTransform { //} func: Array[Any] => Seq[Any], schema: Schema) extends FrameTransform {

  override def work(immutableFrame: ImmutableFrame): Unit = {
    Csv.exportCsvFile(immutableFrame.rdd, fileName, ',')
  }
}

//--------------------------------------------------------------------------------------

trait AddColumnsTrait extends BaseFrame {

  // This isn't necessary to enable ATK, as it will all happen with pyspark.

  /**
   * Add those columns
   */
  def addColumns(func: Array[Any] => Seq[Any], schema: Schema): Unit = {
    execute(AddColumns(func, schema))
  }
}

case class AddColumns(func: Array[Any] => Seq[Any], schema: Schema) extends FrameTransform {

  override def work(immutableFrame: ImmutableFrame): ImmutableFrame = ??? // todo: add Scala support for add_columns

}

//--------------------------------------------------------------------------------------

trait CountTrait extends BaseFrame {

  def count(): Long = execute[Long](Count)
}

case object Count extends FrameSummarization[Long] { // CountSummarization, name must be different from the trait

  def work(frame: ImmutableFrame): Long = frame.rdd.count()
}

//--------------------------------------------------------------------------------------

//--------------------------------------------------------------------------------------

trait ModelRelatedRdd {

}

class KMeansModel(arg4: Int) extends ModelRelatedRdd {

  //val sparkClass : SparkClass   // access mllib's model directly

  def train(frame: Frame, arg1: String, arg2: String): Unit = {

  }

  def score(row: Array[Any]): Array[Any] = {
    null
  }

  def predict(frame: Frame, args: Option[String]): Unit = {
    // modify frame
  }

  def toJson: String = {
    "I am Json."
  }

}

class ModelPublisher {

}
