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
package org.trustedanalytics.sparktk.tensorflow

import java.io.File

import org.tensorflow.hadoop.shaded.protobuf.ByteString
import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{ GenericRow, GenericRowWithSchema }
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ DataTypes => _, _ }
import org.scalatest.Matchers
import org.tensorflow.example._
import org.trustedanalytics.sparktk.frame._
import org.trustedanalytics.sparktk.frame.internal.constructors.ImportTensorflow
import org.trustedanalytics.sparktk.frame.internal.serde.{ DefaultTfRecordRowDecoder, DefaultTfRecordRowEncoder }
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec
import org.trustedanalytics.sparktk.frame.DataTypes._

import scala.collection.JavaConverters._

class TensorFlowTest extends TestingSparkContextWordSpec with Matchers {

  "Spark-tk TensorFlow module" should {
    "Test Import/Export" in {

      val path = "../integration-tests/tests/sandbox/output25.tfr"
      FileUtils.deleteQuietly(new File(path))
      val testRows: Array[Row] = Array(
        new GenericRow(Array[Any](11, 1, 23L, 10.0F, 14.0, Vector(1.0, 2.0), "r1")),
        new GenericRow(Array[Any](21, 2, 24L, 12.0F, 15.0, Vector(2.0, 2.0), "r2")))
      val schema = new FrameSchema(List(Column("id", int32), Column("int32label", int32), Column("int64label", int64), Column("float32label", float32), Column("float64label", float64), Column("vectorlabel", vector(2)), Column("name", string)))
      val rdd = sparkContext.parallelize(testRows)
      val frame = new Frame(rdd, schema)
      frame.exportToTensorflow(path)
      val importedFrame = ImportTensorflow.importTensorflow(sparkContext, path)
      val expectedRows = frame.dataframe.collect()
      val actualDf = importedFrame.dataframe.select("id", "int32label", "int64label", "float32label", "float64label", "vectorlabel", "name")
      val actualRows = actualDf.collect()
      actualRows should equal(expectedRows)
    }

    "Encode given Row as TensorFlow example" in {
      val schemaStructType = StructType(Array(
        StructField("int32label", IntegerType),
        StructField("int64label", LongType),
        StructField("float32label", FloatType),
        StructField("float64label", DoubleType),
        StructField("vectorlabel", ArrayType(DoubleType, false)),
        StructField("strlabel", StringType)
      ))
      val doubleArray = Array(1.1, 111.1, 11111.1)
      val expectedFloatArray = Array(1.1F, 111.1F, 11111.1F)

      val rowWithSchema = new GenericRowWithSchema(Array[Any](1, 23L, 10.0F, 14.0, doubleArray, "r1"), schemaStructType)

      //Encode Sql Row to TensorFlow example
      val example = DefaultTfRecordRowEncoder.encodeTfRecord(rowWithSchema)
      import org.tensorflow.example.Feature

      //Verify each Datatype converted to TensorFlow datatypes
      example.getFeatures.getFeatureMap.asScala.foreach {
        case (featureName, feature) =>
          featureName match {
            case "int32label" => {
              assert(feature.getKindCase.getNumber == Feature.INT64_LIST_FIELD_NUMBER)
              assert(feature.getInt64List.getValue(0).toInt == 1)
            }
            case "int64label" => {
              assert(feature.getKindCase.getNumber == Feature.INT64_LIST_FIELD_NUMBER)
              assert(feature.getInt64List.getValue(0).toInt == 23)
            }
            case "float32label" => {
              assert(feature.getKindCase.getNumber == Feature.FLOAT_LIST_FIELD_NUMBER)
              assert(feature.getFloatList.getValue(0) == 10.0F)
            }
            case "float64label" => {
              assert(feature.getKindCase.getNumber == Feature.FLOAT_LIST_FIELD_NUMBER)
              assert(feature.getFloatList.getValue(0) == 14.0F)
            }
            case "vectorlabel" => {
              assert(feature.getKindCase.getNumber == Feature.FLOAT_LIST_FIELD_NUMBER)
              assert(feature.getFloatList.getValueList.toArray === expectedFloatArray)
            }
            case "strlabel" => {
              assert(feature.getKindCase.getNumber == Feature.BYTES_LIST_FIELD_NUMBER)
              assert(feature.getBytesList.toByteString.toStringUtf8.trim == "r1")
            }
          }
      }
    }

    "Throw null pointer exception for a vector with null values during Encode" in {
      intercept[NullPointerException] {
        val schemaStructType = StructType(Array(
          StructField("vectorlabel", ArrayType(DoubleType, false))
        ))
        val doubleArray = Array(1.1, null, 111.1, null, 11111.1)

        val rowWithSchema = new GenericRowWithSchema(Array[Any](doubleArray), schemaStructType)

        //Throws NullPointerException
        DefaultTfRecordRowEncoder.encodeTfRecord(rowWithSchema)
      }
    }

    "Decode given TensorFlow Example as Row" in {

      //Here Vector with null's are not supported
      val expectedRow = new GenericRow(Array[Any](1, 23L, 10.0F, 14.0, Vector(1.0, 2.0), "r1"))
      val schema = new FrameSchema(List(Column("int32label", int32), Column("int64label", int64), Column("float32label", float32), Column("float64label", float64), Column("vectorlabel", vector(2)), Column("strlabel", string)))

      //Build example
      val intFeature = Int64List.newBuilder().addValue(1)
      val longFeature = Int64List.newBuilder().addValue(23L)
      val floatFeature = FloatList.newBuilder().addValue(10.0F)
      val doubleFeature = FloatList.newBuilder().addValue(14.0F)
      val vectorFeature = FloatList.newBuilder().addValue(1F).addValue(2F).build()
      val strFeature = BytesList.newBuilder().addValue(ByteString.copyFrom("r1".getBytes)).build()
      val features = Features.newBuilder()
        .putFeature("int32label", Feature.newBuilder().setInt64List(intFeature).build())
        .putFeature("int64label", Feature.newBuilder().setInt64List(longFeature).build())
        .putFeature("float32label", Feature.newBuilder().setFloatList(floatFeature).build())
        .putFeature("float64label", Feature.newBuilder().setFloatList(doubleFeature).build())
        .putFeature("vectorlabel", Feature.newBuilder().setFloatList(vectorFeature).build())
        .putFeature("strlabel", Feature.newBuilder().setBytesList(strFeature).build())
        .build()
      val example = Example.newBuilder()
        .setFeatures(features)
        .build()

      //Decode TensorFlow example to Sql Row
      val actualRow = DefaultTfRecordRowDecoder.decodeTfRecord(example, schema)
      actualRow should equal(expectedRow)
    }

    "Check infer schema" in {
      val testRows: Array[Row] = Array(
        new GenericRow(Array[Any](1, Int.MaxValue + 10L, 10.0F, 14.0F, Vector(1.0), "r1")),
        new GenericRow(Array[Any](2, 24, 12.0F, Float.MaxValue + 15, Vector(2.0, 2.0), "r2")))

      //Build example1
      val intFeature1 = Int64List.newBuilder().addValue(1)
      val longFeature1 = Int64List.newBuilder().addValue(Int.MaxValue + 10L)
      val floatFeature1 = FloatList.newBuilder().addValue(10.0F)
      val doubleFeature1 = FloatList.newBuilder().addValue(14.0F)
      val vectorFeature1 = FloatList.newBuilder().addValue(1F).build()
      val strFeature1 = BytesList.newBuilder().addValue(ByteString.copyFrom("r1".getBytes)).build()
      val features1 = Features.newBuilder()
        .putFeature("int32label", Feature.newBuilder().setInt64List(intFeature1).build())
        .putFeature("int64label", Feature.newBuilder().setInt64List(longFeature1).build())
        .putFeature("float32label", Feature.newBuilder().setFloatList(floatFeature1).build())
        .putFeature("float64label", Feature.newBuilder().setFloatList(doubleFeature1).build())
        .putFeature("vectorlabel", Feature.newBuilder().setFloatList(vectorFeature1).build())
        .putFeature("strlabel", Feature.newBuilder().setBytesList(strFeature1).build())
        .build()
      val example1 = Example.newBuilder()
        .setFeatures(features1)
        .build()

      //Build example2
      val intFeature2 = Int64List.newBuilder().addValue(2)
      val longFeature2 = Int64List.newBuilder().addValue(24)
      val floatFeature2 = FloatList.newBuilder().addValue(12.0F)
      val doubleFeature2 = FloatList.newBuilder().addValue(Float.MaxValue + 15)
      val vectorFeature2 = FloatList.newBuilder().addValue(2F).addValue(2F).build()
      val strFeature2 = BytesList.newBuilder().addValue(ByteString.copyFrom("r2".getBytes)).build()
      val features2 = Features.newBuilder()
        .putFeature("int32label", Feature.newBuilder().setInt64List(intFeature2).build())
        .putFeature("int64label", Feature.newBuilder().setInt64List(longFeature2).build())
        .putFeature("float32label", Feature.newBuilder().setFloatList(floatFeature2).build())
        .putFeature("float64label", Feature.newBuilder().setFloatList(doubleFeature2).build())
        .putFeature("vectorlabel", Feature.newBuilder().setFloatList(vectorFeature2).build())
        .putFeature("strlabel", Feature.newBuilder().setBytesList(strFeature2).build())
        .build()
      val example2 = Example.newBuilder()
        .setFeatures(features2)
        .build()

      val exampleRDD: RDD[Example] = sparkContext.parallelize(List(example1, example2))

      val actualSchema = TensorflowInferSchema(exampleRDD)

      //Verify each TensorFlow Datatype is inferred as one of our Datatype
      actualSchema.columns.map { colum =>
        colum.name match {
          case "int32label" => colum.dataType.equalsDataType(DataTypes.int32)
          case "int64label" => colum.dataType.equalsDataType(DataTypes.int64)
          case "float32label" | "float64label" | "vectorlabel" => colum.dataType.equalsDataType(DataTypes.float32)
          case "strlabel" => colum.dataType.equalsDataType(DataTypes.string)
        }
      }
    }
  }
}