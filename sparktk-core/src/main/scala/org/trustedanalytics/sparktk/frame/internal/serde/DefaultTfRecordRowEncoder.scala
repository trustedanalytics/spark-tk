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

import com.google.protobuf.ByteString
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ DoubleType, FloatType, IntegerType, LongType }
import org.tensorflow.example._

trait TfRecordRowEncoder {
  def encodeTfRecord(row: Row): Example
}

object DefaultTfRecordRowEncoder extends TfRecordRowEncoder {

  def encodeTfRecord(row: Row): Example = {
    //maps each column in Row to one of Int64List, FloatList, BytesList based on the column data type
    val features = Features.newBuilder()
    val example = Example.newBuilder()

    row.schema.zipWithIndex.map {
      case (structFiled, index) =>
        structFiled.dataType match {
          case IntegerType => {
            val intResult = Int64List.newBuilder().addValue(row.getInt(index).longValue()).build()
            features.putFeature(structFiled.name, Feature.newBuilder().setInt64List(intResult).build())
          }
          case LongType => {
            val intResult = Int64List.newBuilder().addValue(row.getLong(index).longValue()).build()
            features.putFeature(structFiled.name, Feature.newBuilder().setInt64List(intResult).build())
          }
          case FloatType | DoubleType => {
            val floatResult = FloatList.newBuilder().addValue(row.getFloat(index).floatValue()).build()
            features.putFeature(structFiled.name, Feature.newBuilder().setFloatList(floatResult).build())
          }
          case _ => {
            val strResult = BytesList.newBuilder().addValue(ByteString.copyFrom(row.getString(index).getBytes)).build()
            features.putFeature(structFiled.name, Feature.newBuilder().setBytesList(strResult).build())
          }
        }
    }
    features.build()
    example.setFeatures(features)
    example.build()
  }
}
