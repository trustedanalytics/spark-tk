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
package org.trustedanalytics.sparktk.dicom.internal.ops

import org.scalatest.Matchers
import org.trustedanalytics.sparktk.dicom.internal.constructors.Import._
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec
import org.apache.spark.mllib.linalg.DenseMatrix

class SampleTest extends TestingSparkContextWordSpec with Matchers {

  "SampleTest" should {
    "Test import_dcm" in {
      //      val dicom = importDcm(sparkContext, "../integration-tests/datasets/dicom_uncompressed/mr1.dcm")
      //      println(dicom.pixeldata.take(1)(0)(1))
      val data = Array.ofDim[Double](3, 3)

      var count = 0
      for (i <- 0 until 3) {
        for (j <- 0 until 3) {
          count = count + 1
          data(i)(j) = count
        }
      }
      data.flatten.foreach(println)

      val x = new DenseMatrix(3, 3, data.flatten, isTransposed = true)
      println(x)
    }
  }
}