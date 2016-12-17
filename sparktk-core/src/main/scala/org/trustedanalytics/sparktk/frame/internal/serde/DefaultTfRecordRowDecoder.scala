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
package org.trustedanalytics.sparktk.frame.internal.serde

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.tensorflow.example._
import scala.collection.JavaConverters._
import scala.collection.immutable.Vector
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

import scala.collection.mutable.ArrayBuffer

trait TfRecordRowDecoder {
  def decodeTfRecord(tfExample: Example, schema: Option[StructType] = None): Row
}

object DefaultTfRecordRowDecoder extends TfRecordRowDecoder {

  def decodeTfRecord(tfExample: Example, schema: Option[StructType] = None): Row = {
    //maps each feature in Example to element in Row with DataType based on custom schema or default mapping of  Int64List, FloatList, BytesList to column data type
    val featureMap = tfExample.getFeatures.getFeatureMap.asScala
    val row = new ArrayBuffer[Any]()
    val schema = new ArrayBuffer[StructField]()

    featureMap.foreach {
      case (featureName, feature) =>
        feature.getKindCase.getNumber match {
          case Feature.BYTES_LIST_FIELD_NUMBER => {
            schema += new StructField(featureName, StringType)
            row += feature.getBytesList.toString
          }
          case Feature.INT64_LIST_FIELD_NUMBER => {
            schema += new StructField(featureName, FrameRdd.VectorType)
            row += feature.getInt64List.getValueList.asScala.toArray
          }
          case Feature.FLOAT_LIST_FIELD_NUMBER => {
            schema += new StructField(featureName, FrameRdd.VectorType)
            row += feature.getFloatList.getValueList.asScala.toArray
          }
          case _ => throw new RuntimeException("unsupported type ...")
        }
    }
    new GenericRowWithSchema(row.toArray, StructType(schema))
  }
}
