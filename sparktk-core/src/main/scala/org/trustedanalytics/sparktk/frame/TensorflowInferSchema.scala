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
package org.trustedanalytics.sparktk.frame

import org.apache.spark.rdd.RDD
import org.tensorflow.example._
import org.trustedanalytics.sparktk.frame.DataTypes.{ DataType, float32, float64, int32, int64 }
import scala.collection.mutable.Map
import scala.collection.JavaConverters._
import scala.util.control.Exception._

object TensorflowInferSchema {

  /**
   * Similar to the JSON schema inference.
   * [[org.apache.spark.sql.execution.datasources.json.InferSchema]]
   *     1. Infer type of each row
   *     2. Merge row types to find common type
   *     3. Replace any null types with string type
   */
  def apply(exampleRdd: RDD[Example]): FrameSchema = {
    val startType: Map[String, DataType] = Map.empty[String, DataType]
    val rootTypes: Map[String, DataType] = exampleRdd.aggregate(startType)(inferRowType, mergeFieldTypes)
    val columnsList = rootTypes.map {
      case (featureName, featureType) =>
        if (featureType == null) {
          new Column(featureName, DataTypes.string)
        }
        else {
          new Column(featureName, featureType)
        }
    }
    new FrameSchema(columnsList.toSeq)
  }

  private def inferRowType(schemaSoFar: Map[String, DataType], next: Example): Map[String, DataType] = {
    next.getFeatures.getFeatureMap.asScala.map {
      case (featureName, feature) => {
        val currentType = inferField(feature)
        if (schemaSoFar.contains(featureName)) {
          val updatedType = findTightestCommonType(schemaSoFar(featureName), currentType)
          schemaSoFar(featureName) = updatedType.getOrElse(null)
        }
        else {
          schemaSoFar += (featureName -> currentType)
        }
      }
    }
    schemaSoFar
  }

  private def mergeFieldTypes(first: Map[String, DataType], second: Map[String, DataType]): Map[String, DataType] = {
    //Merge two maps and do the comparision.
    val mutMap = collection.mutable.Map[String, DataType]((first.keySet ++ second.keySet).map(key => (key, findTightestCommonType(first.getOrElse(key, null), second.getOrElse(key, null)).get)).toSeq: _*)
    mutMap
  }

  /**
   * Infer Feature datatype based on filed number
   */
  private def inferField(feature: Feature): DataType = {
    feature.getKindCase.getNumber match {
      case Feature.BYTES_LIST_FIELD_NUMBER => {
        DataTypes.str
      }
      case Feature.INT64_LIST_FIELD_NUMBER => {
        parseInt64List(feature)
      }
      case Feature.FLOAT_LIST_FIELD_NUMBER => {
        parseFloatList(feature)
      }
      case _ => throw new RuntimeException("unsupported type ...")
    }
  }

  private def parseInt64List(feature: Feature): DataType = {
    val int64List = feature.getInt64List.getValueList.asScala.toArray
    val length = int64List.size
    if (length == 0) {
      null
    }
    else if (length > 1) {
      DataTypes.vector(length)
    }
    else {
      val fieldValue = int64List(0).toString
      parseInteger(fieldValue)
    }
  }

  private def parseFloatList(feature: Feature): DataType = {
    val floatList = feature.getFloatList.getValueList.asScala.toArray
    val length = floatList.size
    if (length == 0) {
      null
    }
    else if (length > 1) {
      DataTypes.vector(length)
    }
    else {
      val fieldValue = floatList(0).toString
      parseDouble(fieldValue)
    }
  }

  private def parseInteger(field: String): DataType = if (allCatch.opt(field.toInt).isDefined) {
    DataTypes.int32
  }
  else {
    parseLong(field)
  }

  private def parseLong(field: String): DataType = if (allCatch.opt(field.toLong).isDefined) {
    DataTypes.int64
  }
  else {
    throw new RuntimeException("Unable to parse filed datatype to int64...")
  }

  private def parseDouble(field: String): DataType = {
    if ((allCatch opt field.toDouble).isDefined) {
      DataTypes.float64
    }
    else {
      throw new RuntimeException("Unable to parse field datatype to float64...")
    }
  }

  /**
   * Copied from internal Spark api
   * [[org.apache.spark.sql.catalyst.analysis.HiveTypeCoercion]]
   */
  private val numericPrecedence: IndexedSeq[DataType] =
    IndexedSeq[DataType](int32,
      int64,
      float32,
      float64,
      DataTypes.str)

  private def getNumericPrecedence(dataType: DataType): Int = {
    dataType match {
      case x if x.equals(DataTypes.int32) => 0
      case x if x.equals(DataTypes.int64) => 1
      case x if x.equals(DataTypes.float32) => 2
      case x if x.equals(DataTypes.float64) => 3
      case x if x.isVector => 4
      case x if x.equals(DataTypes.string) => 5
      case _ => throw new RuntimeException("Unable to get the precedence for given datatype...")
    }
  }

  /**
   * Copied from internal Spark api
   * [[org.apache.spark.sql.catalyst.analysis.HiveTypeCoercion]]
   */
  private val findTightestCommonType: (DataType, DataType) => Option[DataType] = {
    case (t1, t2) if t1 == t2 => Some(t1)
    case (null, t2) => Some(t2)
    case (t1, null) => Some(t1)
    case (t1, t2) if t1.isVector && t2.isVector => Some(DataTypes.vector(Math.max(t1.length, t2.length)))
    case (DataTypes.string, t2) => Some(DataTypes.string)
    case (t1, DataTypes.string) => Some(DataTypes.string)

    // Promote numeric types to the highest of the two and all numeric types to unlimited decimal
    case (t1, t2) =>
      val t1Precedence = getNumericPrecedence(t1)
      val t2Precedence = getNumericPrecedence(t2)
      val newType = if (t1Precedence > t2Precedence) t1 else t2
      Some(newType)
    case _ => None
  }
}
