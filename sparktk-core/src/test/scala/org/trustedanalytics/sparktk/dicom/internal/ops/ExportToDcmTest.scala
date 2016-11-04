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

import java.io.File

import org.scalatest.Matchers
import org.trustedanalytics.sparktk.dicom.internal.constructors.Import.importDcm
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class ExportToDcmTest extends TestingSparkContextWordSpec with Matchers {

  "ExportToDcm" should {
    "create new .dcm from metadata and pixeldata and export to specified hdfs/local path" in {
      val dicom = importDcm(sparkContext, "../integration-tests/datasets/dicom_uncompressed")
      val originalMetadataCount = dicom.metadata.rowCount()
      val originalPixeldataCount = dicom.pixeldata.rowCount()

      val destPath = "../integration-tests/tests/sandbox/dicom_export"
      dicom.exportToDcm(destPath)

      val export_status = new File(destPath).exists()
      assert(export_status.equals(true))

      val loadedDicom = importDcm(sparkContext, destPath)
      assert(originalMetadataCount.equals(loadedDicom.metadata.rowCount()))
      assert(originalPixeldataCount.equals(loadedDicom.pixeldata.rowCount()))

    }
  }
}
