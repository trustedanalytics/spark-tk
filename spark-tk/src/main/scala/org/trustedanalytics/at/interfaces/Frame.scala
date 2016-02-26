package org.trustedanalytics.at.interfaces

import net.razorvine.pickle.Pickler
import org.apache.spark.SparkContext
import org.apache.spark.api.java.{ JavaRDD, JavaSparkContext }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.trustedanalytics.at.schema.{ FrameSchema, Schema }

import scala.collection.mutable

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

  def createEmptyFrame(): Frame = {
    //new Frame(sc.emptyRDD: RDD[Row], null)
    val schema = FrameSchema()
    new Frame(PythonSerialization.toRowRdd(sc.emptyRDD: RDD[Array[Any]], schema), schema)
  }

  //def xcount(jrdd: JavaRDD[Row]): Long = jrdd.count()

  //def xtake(jrdd: JavaRDD[Row]): String = jrdd.rdd.map(row => s"$row").collect().mkString("|")

  def sayHello(): String = "Hello from TK"

}

//--------------------------------------------------------------------------------------

//--------------------------------------------------------------------------------------

class Frame(rdd: RDD[Row], schema: Schema) extends BaseFrame
    //class Frame(rdd: RDD[Array[Any]], schema: Schema) extends BaseFrame
    with AddColumnsTrait
    with AppendCsvFile
    with CountTrait
    with Save {
  init(rdd, schema)

  /**
   * (typically called from pyspark, with jrdd)
   * @param rdd
   * @param schema
   */
  //def this(rdd: JavaRDD[Row], schema: Schema) = {
  def this(rdd: JavaRDD[Array[Any]], schema: Schema) = {
    this(PythonSerialization.toRowRdd(rdd.rdd, schema), schema)
  }

  def take(n: Int) = {
    rdd.take(n)
  }
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
  def appendCsvFile(fileName: String, separator: Char): RDD[Row] = { //Array[Any]] = { //func: Array[Any] => Seq[Any], schema: Schema): Unit = {
    execute(AppendCsvFileTransform(rdd.context, fileName, LineParserArguments(separator, null, None)))
  }
}

case class AppendCsvFileTransform(sc: SparkContext, fileName: String, lineParserArguments: LineParserArguments) extends FrameTransform { //} func: Array[Any] => Seq[Any], schema: Schema) extends FrameTransform {

  override def work(immutableFrame: ImmutableFrame): ImmutableFrame = {
    val raa = LoadRddFunctions.loadAndParseLines(sc, fileName, lineParserArguments)
    LoadRddFunctions.toRowRddWithAllStringFields(raa) match {
      case (rdd, schema) => ImmutableFrame(rdd, schema)
    }
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
