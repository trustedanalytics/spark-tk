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

import org.apache.commons.io.FileUtils
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.Row
import org.scalatest.Matchers
import org.tensorflow.example.Example
import org.tensorflow.hadoop.io.TFRecordFileInputFormat
import org.trustedanalytics.sparktk.frame._
import org.trustedanalytics.sparktk.frame.internal.constructors.ImportTensorflow
import org.trustedanalytics.sparktk.frame.internal.serde.{DefaultTfRecordRowDecoder, DefaultTfRecordRowEncoder}
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec
import org.trustedanalytics.sparktk.frame.DataTypes._

class TensorFlowTest extends TestingSparkContextWordSpec with Matchers {

  "Spark-tk TensorFlow module" should {
    "Export frame as TF Records to given dest path" in {

      val destPath = "../integration-tests/tests/sandbox/output25.tfr"
      FileUtils.deleteQuietly(new File(destPath))
      val testRows: Array[Row] = Array(
        new GenericRow(Array[Any](11, 1, 23L, 10.0F, 14.0, Vector(1.0, 2.0), "r1")),
        new GenericRow(Array[Any](21, 2, 24L, 12.0F, 15.0, Vector(2.0, 2.0), "r2")),
        new GenericRow(Array[Any](31, 3, 25L, 13.0F, 16.0, Vector(3.0, 2.0), "r3")),
        new GenericRow(Array[Any](41, 4, 26L, 17.0F, 17.0, Vector(4.0, 2.0), "r4")))
      val schema = new FrameSchema(List(Column("id", int32), Column("int32label", int32), Column("int64label", int64), Column("float32label", float32), Column("float64label", float64), Column("vectorlabel", vector(2)), Column("name", string)))
      val rdd = sparkContext.parallelize(testRows)
      val frame = new Frame(rdd, schema)
      frame.exportToTensorflow(destPath)
      frame.rowCount()
    }

    "Import TF records as spark-tk frame" in {
      val path = "../integration-tests/tests/sandbox/output25.tfr"
      val frame = ImportTensorflow.importTensorflow(sparkContext, path)
      val count = frame.rowCount()
      println(count)
    }

    "Encode given Row as TensorFlow Example" in {
      val testRows: Array[Row] = Array(
        new GenericRow(Array[Any](11, 1, 23L, 10.0F, 14.0, Vector(1.0, 2.0), "r1")),
        new GenericRow(Array[Any](21, 2, 24L, 12.0F, 15.0, Vector(2.0, 2.0), "r2")),
        new GenericRow(Array[Any](31, 3, 25L, 13.0F, 16.0, Vector(3.0, 2.0), "r3")),
        new GenericRow(Array[Any](41, 4, 26L, 17.0F, 17.0, Vector(4.0, 2.0), "r4")))
      val schema = new FrameSchema(List(Column("id", DataTypes.int32), Column("int32label", DataTypes.int32), Column("int64label", DataTypes.int64), Column("float32label", DataTypes.float32), Column("float64label", DataTypes.float64), Column("vectorlabel", DataTypes.vector(2)), Column("name", DataTypes.str)))
      val rdd = sparkContext.parallelize(testRows)
      val frame = new Frame(rdd, schema)

      //Encode Sql Row to TensorFlow example
      val example = DefaultTfRecordRowEncoder.encodeTfRecord(frame.dataframe.first())

    }

    "Decode given TensorFlow Example as Row" in {
      val testRows: Array[Row] = Array(
        new GenericRow(Array[Any](11, 1, 23L, 10.0F, 14.0, Vector(1.0, 2.0), "r1")),
        new GenericRow(Array[Any](21, 2, 24L, 12.0F, 15.0, Vector(2.0, 2.0), "r2")),
        new GenericRow(Array[Any](31, 3, 25L, 13.0F, 16.0, Vector(3.0, 2.0), "r3")),
        new GenericRow(Array[Any](41, 4, 26L, 17.0F, 17.0, Vector(4.0, 2.0), "r4")))
      val schema = new FrameSchema(List(Column("id", DataTypes.int32), Column("int32label", DataTypes.int32), Column("int64label", DataTypes.int64), Column("float32label", DataTypes.float32), Column("float64label", DataTypes.float64), Column("vectorlabel", DataTypes.vector(2)), Column("name", DataTypes.str)))
      val rdd = sparkContext.parallelize(testRows)
      val frame = new Frame(rdd, schema)

      //Encode Sql Row to TensorFlow example
      val example = DefaultTfRecordRowEncoder.encodeTfRecord(frame.dataframe.first())

      //Decode TensorFlow example to Sql Row
      val row = DefaultTfRecordRowDecoder.decodeTfRecord(example, schema)
      row should equal(frame.dataframe.first())
    }

    "Check infer schema" in {
      val path = "../integration-tests/tests/sandbox/output25.tfr"
      val expectedSchema = new FrameSchema(List(Column("id", int32), Column("int32label", int32), Column("int64label", int64), Column("float32label", float32), Column("float64label", float64), Column("vectorlabel", vector(2)), Column("name", string)))
      val rdd = sparkContext.newAPIHadoopFile(path, classOf[TFRecordFileInputFormat], classOf[BytesWritable], classOf[NullWritable])
      val exampleRdd = rdd.map {
        case (bytesWritable, nullWritable) => Example.parseFrom(bytesWritable.getBytes)
      }
      val actualSchema = Some(TensorflowInferSchema(exampleRdd)).get
      //actualSchema should equal(expectedSchema)
    }
  }
}