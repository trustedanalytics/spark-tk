package org.trustedanalytics.sparktk.tensorflow

import com.google.protobuf.ByteString
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.Matchers
import org.tensorflow.example._
import org.tensorflow.hadoop.io.TFRecordFileOutputFormat
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class TestProtoBuf extends TestingSparkContextWordSpec with Matchers {

  "TestProtoBuf" should {
    "Test creating a TF record for .txt file" in {
      val inputPath = "/home/kvadla/spark-tk/spark-tk/integration-tests/datasets/tap.txt"
      val outputPath = "/home/kvadla/spark-tk/spark-tk/integration-tests/tests/sandbox/output.tfr"

      //val sparkConf = new SparkConf().setAppName("TFRecord Demo")
      //val sc = new SparkContext(sparkConf)

      val features = sparkContext.textFile(inputPath).map(line => {
        val text = BytesList.newBuilder().addValue(ByteString.copyFrom(line.getBytes)).build()
        val features = Features.newBuilder()
          .putFeature("text", Feature.newBuilder().setBytesList(text).build())
          .build()
        val example = Example.newBuilder()
          .setFeatures(features)
          .build()
        (new BytesWritable(example.toByteArray), NullWritable.get())
      })

      features.saveAsNewAPIHadoopFile[TFRecordFileOutputFormat](outputPath)
    }
  }
}
