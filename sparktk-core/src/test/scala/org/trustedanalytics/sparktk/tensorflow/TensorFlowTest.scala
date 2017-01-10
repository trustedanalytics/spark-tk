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

import com.google.protobuf.ByteString
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.catalyst.expressions.{GenericRow, GenericRowWithSchema}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes => _, _}
import org.scalatest.Matchers
import org.tensorflow.example._
import org.trustedanalytics.sparktk.frame._
import org.trustedanalytics.sparktk.frame.internal.constructors.ImportTensorflow
import org.trustedanalytics.sparktk.frame.internal.serde.{DefaultTfRecordRowDecoder, DefaultTfRecordRowEncoder}
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec
import org.trustedanalytics.sparktk.frame.DataTypes._

import scala.collection.JavaConverters._
import scala.collection.mutable

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
      val schemaStructType = StructType(Array(StructField("id", IntegerType),
        StructField("int32label", IntegerType),
        StructField("int64label", LongType),
        StructField("float32label", FloatType),
        StructField("float64label", DoubleType),
        //StructField("vectorlabel", ArrayType(DoubleType, false)),
        StructField("name", StringType)
      ))
      //val doubleArray = Array(1.1, null, 111.1, null, 11111.1)
      //val doubleGenericArray =
      //val rowWithSchema = new GenericRowWithSchema(Array[Any](11, 1, 23L, 10.0F, 14.0, doubleGenericArray, "r1"), schemaStructType)
      val rowWithSchema = new GenericRowWithSchema(Array[Any](11, 1, 23L, 10.0F, 14.0, "r1"), schemaStructType)

      //Encode Sql Row to TensorFlow example
      val example = DefaultTfRecordRowEncoder.encodeTfRecord(rowWithSchema)
      import org.tensorflow.example.Feature

      //Verify each Datatype converted to TensorFlow datatypes
      example.getFeatures.getFeatureMap.asScala.foreach {
        case (featureName, feature) =>
          featureName match {
            case "id" | "int32label" | "int64label" => assert(feature.getKindCase.getNumber == Feature.INT64_LIST_FIELD_NUMBER)
            case "float32label" | "float64label" => assert(feature.getKindCase.getNumber == Feature.FLOAT_LIST_FIELD_NUMBER)
            case "name" => assert(feature.getKindCase.getNumber == Feature.BYTES_LIST_FIELD_NUMBER)
          }
      }
    }

    "Decode given TensorFlow Example as Row" in {
      val testRows: Array[Row] = Array(
        new GenericRow(Array[Any](11, 1, 23L, 10.0F, 14.0, Vector(1.0, 2.0), "r1")),
        new GenericRow(Array[Any](21, 2, 24L, 12.0F, 15.0, Vector(2.0, 2.0), "r2")))
      val schema = new FrameSchema(List(Column("id", int32), Column("int32label", int32), Column("int64label", int64), Column("float32label", float32), Column("float64label", float64), Column("vectorlabel", vector(2)), Column("name", string)))
      val rdd = sparkContext.parallelize(testRows)
      val frame = new Frame(rdd, schema)

      //Build example RDD for the frame
      val exampleRDD = frame.dataframe.map{ row =>
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

      //Decode TensorFlow example to Sql Row
      val rowRDD = exampleRDD.map(example => DefaultTfRecordRowDecoder.decodeTfRecord(example, schema))
      frame.rdd.collect() should equal(rowRDD.collect())
    }

    "Check infer schema" in {
      val testRows: Array[Row] = Array(
        new GenericRow(Array[Any](11, 1, 23L, 10.0F, 14.0, Vector(1.0, 2.0), "r1")),
        new GenericRow(Array[Any](21, 2, 24L, 12.0F, 15.0, Vector(2.0, 2.0), "r2")))
      val schema = new FrameSchema(List(Column("id", int32), Column("int32label", int32), Column("int64label", int64), Column("float32label", float32), Column("float64label", float64), Column("vectorlabel", vector(2)), Column("name", string)))
      val rdd = sparkContext.parallelize(testRows)
      val frame = new Frame(rdd, schema)
      //Build example RDD for the frame
      val exampleRDD = frame.dataframe.map{ row =>
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
      val actualSchema = TensorflowInferSchema(exampleRDD)

      //Verify each TensorFlow Datatype is inferred as one of our Datatype
      actualSchema.columns.map { colum =>
        colum.name match {
          case "id" | "int32label" | "int64label" => colum.dataType.equalsDataType(DataTypes.int32)
          case "float32label" | "float64label" | "vectorlabel" => colum.dataType.equalsDataType(DataTypes.float64)
          case "name" => colum.dataType.equalsDataType(DataTypes.string)
        }
      }
    }
  }
}