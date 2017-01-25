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
package org.trustedanalytics.sparktk.jvm

import java.util
import java.util.{ ArrayList => JArrayList, List => JList, Map => JMap }

import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.mllib.linalg.DenseMatrix
import org.joda.time.{ DateTime, DateTimeZone }
import org.apache.spark.org.trustedanalytics.sparktk.SparkAliases
import org.trustedanalytics.sparktk.frame.{ Column, DataTypes, FrameSchema }

import scala.collection.JavaConverters._
//import scala.collection.mutable

object JConvert extends Serializable {

  def toScalaSparkContext(jsc: JavaSparkContext): SparkContext = {
    JavaSparkContext.toSparkContext(jsc)
  }

  def toScalaList[T](x: JList[T]): List[T] = x.asScala.toList

  def toScalaVector[T](x: JList[T]): Vector[T] = x.asScala.toVector

  def toOption[T](item: T) = {
    item match {
      case null | None => None
      case item: JList[T] => Some(toScalaList(item))
      case _ => Some(item)
    }
  }

  def toEitherStringInt(item: Any): Either[String, Int] = {
    item match {
      case s: String => Left(s)
      case i: Int => Right(i)
    }
  }

  def toDateTime(item: Any): DateTime = {
    item match {
      case l: Long => new DateTime(l, DateTimeZone.UTC);
      case s: String => DateTime.parse(s)
      case _ => throw new IllegalArgumentException(s"Unable to translate type ${item.getClass.getName} to DateTime.")
    }
  }

  def fromOption(o: Option[Any]): Any = o.orNull

  def scalaMapToPython[K, V](m: Map[K, V]): JMap[K, V] = SparkAliases.JavaUtils.mapAsSerializableJavaMap(m)

  def scalaSeqToPython[T](seq: Seq[T]): JList[T] = seq.asJava

  def scalaVectorToPython[T](vector: Vector[T]): JList[T] = vector.asJava

  def scalaMatrixToPython(matrix: DenseMatrix): JList[JList[Double]] = {
    val (numRows, numCols) = (matrix.numRows, matrix.numCols)
    val result = new JArrayList[JArrayList[Double]]()
    for (i <- 0 until numRows) {
      val r = new JArrayList[Double]()
      for (j <- 0 until numCols) r.add(matrix(i, j))
      result.add(r)
    }
    result.asInstanceOf[JList[JList[Double]]]
  }

  def toScalaTuple2[T](a: T, b: T): (T, T) = (a, b)

  def toScalaMap[K, V](jm: java.util.Map[K, V]): Map[K, V] = SparkAliases.PythonUtils.toScalaMap(jm)

  def combineScalaMap[K, V](seq: Seq[Map[K, V]]): Map[K, V] = {
    seq.reduce(_ ++ _)
  }

  //  def frameSchemaToScala(pythonSchema: JArrayList[JArrayList[String]]): FrameSchema = {
  //    val columns = pythonSchema.asScala.map { item =>
  //      val list = item.asScala.toList
  //      require(list.length == 2, "Schema entries must be tuples of size 2 (name, dtype)")
  //      Column(list.head, DataTypes.toDataType(list(1)))
  //    }.toVector
  //    FrameSchema(columns)
  //  }
  //
  //    def frameSchemaToPython(scalaSchema: FrameSchema): JArrayList[JArrayList[String]] = {
  //      val pythonSchema = new JArrayList[JArrayList[String]]()
  //      scalaSchema.columns.map {
  //        case Column(name, dtype) =>
  //          val c = new JArrayList[String]()
  //          c.add(name)
  //          c.add(dtype.toString)
  //          pythonSchema.add(c)
  //      }
  //      pythonSchema
  //    }

  /**
   * Convert an RDD of Java objects to an RDD of serialized Python objects, that is usable by
   * PySpark.
   *
   * (private function taken from Spark's org.apache.spark.api.python.SerDeUtil)
   */
  //  def javaToPython(jrdd: JavaRDD[Row]): JavaRDD[Array[Byte]] = {
  //    //def javaToPython(jRDD: JavaRDD[_]): JavaRDD[Array[Byte]] = {
  //    scalaToPython(jrdd.rdd) //.mapPartitions { iter => new AutoBatchedPickler(iter) }
  //  }

  //  def scalaToPython(rdd: RDD[Row], schema: Schema): JavaRDD[Array[Byte]] = {
  //    //def javaToPython(jRDD: JavaRDD[_]): JavaRDD[Array[Byte]] = {
  //    println("In scalaToPython...")
  //    //SerDeUtil.javaToPython(rdd.toJavaRDD())
  //
  //    //# these two lines work, but rows show up as tuples...
  //    val raa: RDD[Any] = toAnyRDD(rdd, schema) // , rdd)
  //    EvaluatePython.javaToPython(raa)
  //
  //    // don't know about these two lines:
  //    //rdd.mapPartitions { iter => EvaluatePython.rowToArray}
  //    //rdd.mapPartitions { iter => new AutoBatchedPickler(iter) }
  //  }
  //
  //  def toAnyRDD(rdd: RDD[Row], schema: Schema): RDD[Any] = {
  //    val anyRDD: RDD[Any] = rdd.map(row => {
  //      //      val rowArray = new Array[Any](row.length)
  //      //      row.toSeq.zipWithIndex.map {
  //      //        case (o, i) =>
  //      //          o match {
  //      //            case null => null
  //      //            case _ => rowArray(i) = o
  //      //            //              val colType = schema.column(i).dataType
  //      //            //              val dType: DataType = FrameRdd.schemaDataTypeToSqlDataType(colType)
  //      //            //              try {
  //      //            //                rowArray(i) = EvaluatePython.fromJava(o, dType)
  //      //            //              }
  //      //            //              catch {
  //      //            //                case e: Exception => null
  //      //            //              }
  //      //          }
  //      //      }.toArray
  //      //      rowArray
  //      row.toSeq.toArray
  //      //new GenericRow(rowArray)
  //    })
  //    anyRDD
  //  }
  //
  //  def pythonToScala(jrdd: JavaRDD[Array[Byte]], schema: Schema): RDD[Row] = {
  //    val raa: JavaRDD[Array[Any]] = pythonToJava(jrdd)
  //    toRowRdd(raa.rdd, schema)
  //  }
  //
  //  def pythonToJava(jrdd: JavaRDD[Array[Byte]]): JavaRDD[Array[Any]] = {
  //    val j = SerDeUtil.pythonToJava(jrdd, batched = true)
  //    SerDeUtil.toJavaArrayAny(j)
  //  }
  //
  //  def toRowRdd(raa: RDD[Array[Any]], schema: Schema): RDD[org.apache.spark.sql.Row] = {
  //    val rowRDD: RDD[org.apache.spark.sql.Row] = raa.map(row => {
  //      val rowArray = new Array[Any](row.length)
  //      row.zipWithIndex.map {
  //        case (o, i) =>
  //          o match {
  //            case null => null
  //            case _ =>
  //              val colType = schema.column(i).dataType
  //              try {
  //                rowArray(i) = colType.parse(o).get
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
}
