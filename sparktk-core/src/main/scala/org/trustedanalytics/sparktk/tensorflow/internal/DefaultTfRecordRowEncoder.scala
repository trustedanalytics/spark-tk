package org.trustedanalytics.sparktk.tensorflow.internal

import com.google.protobuf.ByteString
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, LongType}
import org.tensorflow.example._

trait TfRecordRowEncoder {
  def encodeTfRecord(row: Row): Example
}

object DefaultTfRecordRowEncoder extends TfRecordRowEncoder {

  def encodeTfRecord(row: Row): Example = {
    //maps each column in Row to one of Int64List, FloatList, BytesList based on the column data type
    val features = Features.newBuilder()
    val example = Example.newBuilder()

    row.schema.zipWithIndex.map { case (structFiled, index) =>
      structFiled.dataType match {
        case IntegerType | LongType=> {
          val intResult = Int64List.newBuilder().addValue(row.get(index).asInstanceOf[Long]).build()
          features.putFeature(structFiled.name, Feature.newBuilder().setInt64List(intResult).build())
        }
        case FloatType | DoubleType => {
          val floatResult = FloatList.newBuilder().addValue(row.get(index).asInstanceOf[Float]).build()
          features.putFeature(structFiled.name, Feature.newBuilder().setFloatList(floatResult).build())
        }
        case _ => {
          val strResult = BytesList.newBuilder().addValue(ByteString.copyFrom(row.get(index).toString.getBytes)).build()
          features.putFeature(structFiled.name, Feature.newBuilder().setBytesList(strResult).build())
        }
      }
    }
    features.build()
    example.setFeatures(features)
    example.build()
  }
}
