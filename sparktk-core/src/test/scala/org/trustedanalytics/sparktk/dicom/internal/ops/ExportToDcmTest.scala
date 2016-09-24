package org.trustedanalytics.sparktk.dicom.internal.ops

import org.scalatest.Matchers
import org.trustedanalytics.sparktk.dicom.internal.constructors.Import.importDcm
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class ExportToDcmTest extends TestingSparkContextWordSpec with Matchers {

  "ExportToDcm" should {
    "create new .dcm from metadata and pixeldata and export to specified hdfs/local path" in {
      val dicom = importDcm(sparkContext, "../integration-tests/datasets/dicom_uncompressed")

      dicom.exportToDcm("dicom_export")
    }
  }
}
