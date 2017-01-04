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
import org.apache.spark.sql.types._
import org.tensorflow.example._
import org.trustedanalytics.sparktk.frame.DataTypes

import scala.collection.mutable

trait TfRecordRowEncoder {
  /**
   * Encodes each Row as TensorFlow "Example"
   *
   * Maps each column in Row to one of Int64List, FloatList, BytesList based on the column data type
   *
   * @param row a frame row
   * @return TensorFlow Example
   */
  def encodeTfRecord(row: Row): Example
}

object DefaultTfRecordRowEncoder extends TfRecordRowEncoder {

  def encodeTfRecord(row: Row): Example = {

    val features = Features.newBuilder()
    val example = Example.newBuilder()

    val schema = row.schema
    row.schema.zipWithIndex.map {
      case (structField, index) =>
        structField.dataType match {
          case IntegerType => {
            val intResult = Int64List.newBuilder().addValue(DataTypes.toLong(row.getInt(index))).build()
            features.putFeature(structField.name, Feature.newBuilder().setInt64List(intResult).build())
          }
          case LongType => {
            val intResult = Int64List.newBuilder().addValue(DataTypes.toLong(row.getLong(index))).build()
            features.putFeature(structField.name, Feature.newBuilder().setInt64List(intResult).build())
          }
          case FloatType => {
            val floatResult = FloatList.newBuilder().addValue(DataTypes.toFloat(row.getFloat(index))).build()
            features.putFeature(structField.name, Feature.newBuilder().setFloatList(floatResult).build())
          }
          case DoubleType => {
            val floatResult = FloatList.newBuilder().addValue(DataTypes.toFloat(row.getDouble(index))).build()
            features.putFeature(structField.name, Feature.newBuilder().setFloatList(floatResult).build())
          }
          case ArrayType(DoubleType, false) => {
            val wrappedArr = row.getAs[mutable.WrappedArray[Double]](index)
            val floatListBuilder = FloatList.newBuilder()
            wrappedArr.foreach(x => floatListBuilder.addValue(x.toFloat))
            val floatList = floatListBuilder.build()
            features.putFeature(structField.name, Feature.newBuilder().setFloatList(floatList).build())
          }
          case _ => {
            val byteList = BytesList.newBuilder().addValue(ByteString.copyFrom(row.getString(index).getBytes)).build()
            features.putFeature(structField.name, Feature.newBuilder().setBytesList(byteList).build())
          }
        }
    }
    features.build()
    example.setFeatures(features)
    example.build()
  }
}
