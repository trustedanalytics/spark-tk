package org.trustedanalytics.at.frame.internal.rdd

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.execution.EvaluatePython
import org.apache.spark.org.trustedanalytics.at.frame.PrivateSparkLib
import java.util.{ ArrayList => JArrayList }
import org.trustedanalytics.at.frame.Schema

import scala.collection.JavaConversions._

/**
 * For converting RDDs between Python and Scala
 */
object PythonJavaRdd {

  def scalaToPython(rdd: RDD[Row], schema: Schema): JavaRDD[Array[Byte]] = {
    //def javaToPython(jRDD: JavaRDD[_]): JavaRDD[Array[Byte]] = {
    println("In scalaToPython...")
    //SerDeUtil.javaToPython(rdd.toJavaRDD())

    //# these two lines work, but rows show up as tuples...  // todo - figure out more here
    val raa: RDD[Any] = toAnyRdd(rdd, schema)
    EvaluatePython.javaToPython(raa)

    // don't know about these two lines:
    //rdd.mapPartitions { iter => EvaluatePython.rowToArray}
    //rdd.mapPartitions { iter => new AutoBatchedPickler(iter) }
  }

  def pythonToScala(jrdd: JavaRDD[Array[Byte]], scalaSchema: Schema): RDD[Row] = {
    val raa: JavaRDD[Array[Any]] = pythonToJava(jrdd)
    toRowRdd(raa.rdd, scalaSchema)
  }

  private def pythonToJava(jrdd: JavaRDD[Array[Byte]]): JavaRDD[Array[Any]] = {
    val j = PrivateSparkLib.SerDeUtil.pythonToJava(jrdd, batched = true)
    toJavaArrayAnyRdd(j)
  }

  def toJavaArrayAnyRdd(jrdd: RDD[Any]): JavaRDD[Array[Any]] = {
    toArrayAnyRdd(jrdd).toJavaRDD()
  }

  def toArrayAnyRdd(rdd: RDD[_]): RDD[Array[Any]] = {
    rdd.map {
      case objs: JArrayList[_] =>
        objs.map(item => item.asInstanceOf[Any]).toArray
      //objs.toSeq.toArray  //.map(item => item.asInstanceOf[Any]).toArray
      case obj if obj.getClass.isArray =>
        //obj.asInstanceOf[Array[_]].map(item => item.asInstanceOf[Any]).toArray
        obj.asInstanceOf[Array[_]].map(item => item.asInstanceOf[Any]).toArray
      //obj.toList.map(item => item.asInstanceOf[Any]).toArray
    }
  }

  def toAnyRdd(rdd: RDD[Row], schema: Schema): RDD[Any] = {
    val anyRdd: RDD[Any] = rdd.map(row => { // todo - Is there a better way to just cast to RDD[Any]?
      row.toSeq.toArray
    })
    anyRdd
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
                rowArray(i) = colType.parse(o).get
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
