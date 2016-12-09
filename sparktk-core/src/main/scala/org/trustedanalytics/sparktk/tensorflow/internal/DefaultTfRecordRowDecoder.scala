package org.trustedanalytics.sparktk.tensorflow.internal

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.tensorflow.example._
import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

import scala.collection.mutable.ArrayBuffer

trait TfRecordRowDecoder {
  def decodeTfRecord(tfExample: Example, schema: Option[StructType] = None):Row
}

object DefaultTfRecordRowDecoder extends TfRecordRowDecoder {

  def decodeTfRecord(tfExample: Example, schema: Option[StructType] = None):Row = {
    //maps each feature in Example to element in Row with DataType based on custom schema or default mapping of  Int64List, FloatList, BytesList to column data type
    val featureMap = tfExample.getFeatures.getFeatureMap.asScala
    val row = new ArrayBuffer[Any]()
    val schema = new ArrayBuffer[StructField]()

    featureMap.foreach { case (featureName, feature) =>
      feature.getKindCase.getNumber match {
        case Feature.BYTES_LIST_FIELD_NUMBER => {
          schema += new StructField(featureName, StringType)
          row += feature.getBytesList.toString
        }
        case Feature.INT64_LIST_FIELD_NUMBER => {
          schema += new StructField(featureName, ArrayType(LongType))
          row += feature.getInt64List.getValueList.asScala.toArray
        }
        case Feature.FLOAT_LIST_FIELD_NUMBER => {
          schema += new StructField(featureName, ArrayType(LongType))
          row += feature.getInt64List.getValueList.asScala.toArray
        }
        case _ => throw new RuntimeException("unsupported type ...")
      }
    }
    new GenericRowWithSchema(row.toArray, StructType(schema))
  }
}
