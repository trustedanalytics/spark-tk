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

import org.apache.spark.sql.SQLContext
import org.scalatest.Matchers
import org.trustedanalytics.sparktk.frame.internal.constructors.ImportTensorflow
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class TestProtoBuf extends TestingSparkContextWordSpec with Matchers {

  "TestProtoBuf" should {
    "Test creating a TF record for .txt file" in {

      val inputPath = "/home/kvadla/spark-tk/spark-tk/integration-tests/datasets/people.json"
      val destPath = "/home/kvadla/spark-tk/spark-tk/integration-tests/tests/sandbox/output26.tfr"
      val sqlContext = new SQLContext(sparkContext)
      import sqlContext.implicits._
      val sourceDf = sqlContext.read.json(inputPath)
      ExportToTfRecord.exportToTfRecord(sourceDf, destPath)

    }

    "read test tf" in {
      val path = "/home/kvadla/spark-tk/spark-tk/integration-tests/tests/sandbox/output26.tfr"
      val frame = ImportTensorflow.importTensorflow(sparkContext, path)
      frame
    }
  }
}
