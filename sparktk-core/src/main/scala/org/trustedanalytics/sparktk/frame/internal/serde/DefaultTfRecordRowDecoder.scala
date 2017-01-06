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
import scala.collection.JavaConverters._
import org.trustedanalytics.sparktk.frame.{ DataTypes, FrameSchema }

trait TfRecordRowDecoder {
  /**
   * Decodes each TensorFlow "Example" as Frame "Row"
   *
   * Maps each feature in Example to element in Row with DataType based on custom schema or default mapping of Int64List, FloatList, BytesList to column data type
   *
   * @param example TensorFlow Example to decode
   * @param schema Decode Example using specified schema
   * @return a frame row
   */
  def decodeTfRecord(example: Example, schema: FrameSchema): Row
}

object DefaultTfRecordRowDecoder extends TfRecordRowDecoder {

  def decodeTfRecord(example: Example, schema: FrameSchema): Row = {
    val row = Array.fill[Any](schema.columns.length)(null)
    example.getFeatures.getFeatureMap.asScala.foreach {
      case (featureName, feature) =>
        val colDataType = schema.columnDataType(featureName)
        row(schema.columnIndex(featureName)) = colDataType match {
          case vtype if vtype.equals(DataTypes.int32) | vtype.equals(DataTypes.int64) => {
            val dataList = feature.getInt64List.getValueList
            if (dataList.size() == 1)
              colDataType.parse(dataList.get(0)).get
            else
              throw new RuntimeException("Mismatch in schema type, expected int32 or int64")
          }
          case vtype if vtype.equals(DataTypes.float32) | vtype.equals(DataTypes.float64) => {
            val dataList = feature.getFloatList.getValueList
            if (dataList.size() == 1)
              colDataType.parse(dataList.get(0)).get
            else
              throw new RuntimeException("Mismatch in schema type, expected float64")
          }
          case vtype: DataTypes.vector => {
            feature.getKindCase.getNumber match {
              case Feature.INT64_LIST_FIELD_NUMBER => {
                val dataList = feature.getInt64List.getValueList
                if (vtype.length == dataList.size())
                  vtype.parse(dataList.asScala.toList).get
                else
                  throw new RuntimeException("Mismatch in vector length...")
              }
              case Feature.FLOAT_LIST_FIELD_NUMBER => {
                val dataList = feature.getFloatList.getValueList
                if (vtype.length == dataList.size())
                  vtype.parse(dataList.asScala.toList).get
                else
                  throw new RuntimeException("Mismatch in vector length...")
              }
            }
          }
          case x if x.equals(DataTypes.string) => feature.getBytesList.toByteString.toStringUtf8.trim
        }
    }
    Row.fromSeq(row)
  }
}
