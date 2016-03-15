package org.trustedanalytics.at.jconvert

import java.util.{ ArrayList => JArrayList }

//import net.razorvine.pickle.Pickler
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.execution.EvaluatePython
import org.trustedanalytics.at.schema.{ Column, DataTypes, FrameSchema, Schema }

import scala.collection.JavaConverters._
//import scala.collection.mutable

object PythonConvert extends Serializable {

  def toScalaListDouble(x: JArrayList[Double]): List[Double] = x.asScala.toList

  def toScalaListString(x: JArrayList[String]): List[String] = x.asScala.toList

  def toScalaVectorDouble(x: JArrayList[Double]): Vector[Double] = x.asScala.toVector

  def toScalaVectorString(x: JArrayList[String]): Vector[String] = x.asScala.toVector

  def noneOption() = None
  def someOptionString(s: String) = Some(s)
  def someOptionInt(i: Int) = Some(i)
  def someOptionDouble(d: Double) = Some(d)

  def scalaMapStringIntToPython(m: Map[String, Int]) = {
    scalaMapToPython[String, Int](m)
  }

  def scalaMapToPython[K, V](m: Map[K, V]): JArrayList[JArrayList[String]] = {
    val pythonMap = new JArrayList[JArrayList[String]]()
    m.map {
      case (k, v) =>
        val entry = new JArrayList[String]()
        entry.add(k.toString)
        entry.add(v.toString)
        pythonMap.add(entry)
    }
    pythonMap
  }

  def frameSchemaToScala(pythonSchema: JArrayList[JArrayList[String]]): Schema = {
    val columns = pythonSchema.asScala.map { item =>
      val list = item.asScala.toList
      require(list.length == 2, "Schema entries must be tuples of size 2 (name, dtype)")
      Column(list.head, DataTypes.toDataType(list(1)))
    }.toVector
    FrameSchema(columns)
  }

  def frameSchemaToPython(scalaSchema: FrameSchema): JArrayList[JArrayList[String]] = {
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

  /**
   * Convert an RDD of Java objects to an RDD of serialized Python objects, that is usable by
   * PySpark.
   *
   * (private function taken from Spark's org.apache.spark.api.python.SerDeUtil)
   */
  def javaToPython(jrdd: JavaRDD[Row]): JavaRDD[Array[Byte]] = {
    //def javaToPython(jRDD: JavaRDD[_]): JavaRDD[Array[Byte]] = {
    scalaToPython(jrdd.rdd) //.mapPartitions { iter => new AutoBatchedPickler(iter) }
  }

  def scalaToPython(rdd: RDD[Row]): JavaRDD[Array[Byte]] = {
    //def javaToPython(jRDD: JavaRDD[_]): JavaRDD[Array[Byte]] = {
    println("In scalaToPython...")
    //val raa = toAnyRDD(schema, rdd)
    EvaluatePython.javaToPython(null) //rdd)
    //rdd.mapPartitions { iter => EvaluatePython.rowToArray}
    //rdd.mapPartitions { iter => new AutoBatchedPickler(iter) }
  }

  def toAnyRDD(rdd: RDD[Row], schema: Schema): RDD[Array[Any]] = {
    val anyRDD: RDD[Array[Any]] = rdd.map(row => {
      val rowArray = new Array[Any](row.length)
      row.toSeq.zipWithIndex.map {
        case (o, i) =>
          o match {
            case null => null
            case _ =>
              val colType = schema.column(i).dataType
              //val dType: DataType = colType.scalaType
              try {
                rowArray(i) = o.asInstanceOf[colType.ScalaType]
                //rowArray(i) = EvaluatePython.toJava(o, dType)
              }
              catch {
                case e: Exception => null
              }
          }
      }.toArray
      //new GenericRow(rowArray)
    })
    anyRDD
  }

  def pythonToScala(jrdd: JavaRDD[Array[Byte]], schema: Schema): RDD[Row] = {
    //jrdd.saveAsTextFile("/home/blbarker/tmp/jrdd")
    val raa: JavaRDD[Array[Any]] = pythonToJava(jrdd)
    //jrdd.saveAsTextFile("/home/blbarker/tmp/raa")
    val rdd = toRowRdd(raa.rdd, schema)
    //rdd.saveAsTextFile("/home/blbarker/tmp/rdd")
    rdd
  }

  def pythonToJava(jrdd: JavaRDD[Array[Byte]]): JavaRDD[Array[Any]] = {
    val j = SerDeUtil.pythonToJava(jrdd, batched = true)
    SerDeUtil.toJavaArrayAny(j)
  }

  def toRowRdd(raa: RDD[Array[Any]], schema: Schema): RDD[org.apache.spark.sql.Row] = {
    val rowRDD: RDD[org.apache.spark.sql.Row] = raa.map(row => {
      val rowArray = new Array[Any](row.length)
      row.zipWithIndex.map {
        case (o, i) =>
          o match {
            case null => null
            case _ =>
              val colType = schema.column(i).dataType
              try {
                rowArray(i) = o.asInstanceOf[colType.ScalaType]
              }
              catch {
                case e: Exception => null
              }
          }
      }
      new GenericRow(rowArray)
    })
    rowRDD
  }
}
