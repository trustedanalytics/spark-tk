package org.trustedanalytics.sparktk.tensorflow

import org.apache.spark.sql.SQLContext
import org.scalatest.Matchers
import org.trustedanalytics.sparktk.tensorflow.internal.constructors.Import
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec
import org.trustedanalytics.sparktk.tensorflow.internal.ops.ExportToTfRecord


class TestProtoBuf extends TestingSparkContextWordSpec with Matchers {

  "TestProtoBuf" should {
    "Test creating a TF record for .txt file" in {

      val inputPath = "/home/kvadla/spark-tk/spark-tk/integration-tests/datasets/people.json"
      val destPath = "/home/kvadla/spark-tk/spark-tk/integration-tests/tests/sandbox/output26.tfr"
      val sqlContext = new SQLContext(sparkContext)
      import sqlContext.implicits._
      val sourceDf =sqlContext.read.json(inputPath)
      ExportToTfRecord.exportToTfRecord(sourceDf, destPath)

    }

    "read test tf" in {
      val path = "/home/kvadla/spark-tk/spark-tk/integration-tests/tests/sandbox/output26.tfr"
      val rdd = Import.importTfRecord(sparkContext, path)
      rdd.foreach(println)
    }
}
}
