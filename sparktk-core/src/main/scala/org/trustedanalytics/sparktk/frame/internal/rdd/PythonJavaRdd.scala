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
package org.trustedanalytics.sparktk.frame.internal.rdd

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.linalg.DenseMatrix
import org.apache.spark.org.trustedanalytics.sparktk.SparkAliases
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import java.util.{ ArrayList => JArrayList }
import org.trustedanalytics.sparktk.frame.Schema
import org.trustedanalytics.sparktk.jvm.JConvert

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import java.util.{ List => JList }

/**
 * For converting RDDs between Python and Scala
 */
object PythonJavaRdd {

  def convertToJavaSeq(row: Row): JList[_] = {
    row.toSeq.map {
      case x: DenseMatrix => JConvert.scalaMatrixToPython(x)
      case y => y
    }.asJava
  }

  def scalaToPython(rdd: RDD[Row]): JavaRDD[Array[Byte]] = {
    rdd.map(convertToJavaSeq).mapPartitions { iter => new SparkAliases.AutoBatchedPickler(iter) }
  }

  def pythonToScala(jrdd: JavaRDD[Array[Byte]], scalaSchema: Schema): RDD[Row] = {
    val raa: JavaRDD[Array[Any]] = pythonToJava(jrdd)
    toRowRdd(raa.rdd, scalaSchema)
  }

  private def pythonToJava(jrdd: JavaRDD[Array[Byte]]): JavaRDD[Array[Any]] = {
    // calling SparkAliases.MLLibSerDe, internally registers DenseVectorPickler, DenseMatrixPickler, SparseMatrixPickler, SparseVectorPickler for serialization purpose.
    val j = SparkAliases.MLLibSerDe.pythonToJava(jrdd, batched = true)
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
