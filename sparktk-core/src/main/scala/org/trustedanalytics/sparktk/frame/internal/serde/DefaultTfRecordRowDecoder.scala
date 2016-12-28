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
import org.tensorflow.example._
import org.trustedanalytics.sparktk.frame.{ DataTypes, FrameSchema }

trait TfRecordRowDecoder {
  def decodeTfRecord(featureMap: Map[String, Feature], schema: FrameSchema): Row
}

object DefaultTfRecordRowDecoder extends TfRecordRowDecoder {

  def decodeTfRecord(featureMap: Map[String, Feature], schema: FrameSchema): Row = {
    //maps each feature in Example to element in Row with DataType based on custom schema or default mapping of  Int64List, FloatList, BytesList to column data type

    val row = Array.fill[Any](schema.columns.length)(null)
    featureMap.foreach {
      case (featureName, feature) =>
        val colDataType = schema.columnDataType(featureName)
        colDataType match {
          case x if x.equals(DataTypes.int32) | x.equals(DataTypes.int64) => {
            row(schema.columnIndex(featureName)) = colDataType.parse(feature.getInt64List.getValue(0))
          }
          case x if x.equals(DataTypes.float64) => {
            row(schema.columnIndex(featureName)) = colDataType.parse(feature.getFloatList.getValue(0))
          }
          case x if x.isVector => {
            feature.getKindCase.getNumber match {
              case Feature.INT64_LIST_FIELD_NUMBER => row(schema.columnIndex(featureName)) = colDataType.parse(feature.getInt64List.getValueList)
              case Feature.FLOAT_LIST_FIELD_NUMBER => row(schema.columnIndex(featureName)) = colDataType.parse(feature.getFloatList.getValueList)
            }
          }
          case x if x.equals(DataTypes.string) => {
            row(schema.columnIndex(featureName)) = feature.getBytesList.toString
          }
        }
    }
    Row.fromSeq(row)
  }
}
