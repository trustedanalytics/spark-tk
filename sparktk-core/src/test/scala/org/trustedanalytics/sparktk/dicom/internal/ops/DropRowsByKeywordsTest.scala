package org.trustedanalytics.sparktk.dicom.internal.ops

import org.scalatest.Matchers
import org.trustedanalytics.sparktk.dicom.internal.constructors.Import._
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class DropRowsByKeywordsTest extends TestingSparkContextWordSpec with Matchers {

  "DropRowsByKeywords" should {
    "Drop the rows based on Map(keyword, value) from column holding xml string" in {
      val dicom = importDcm(sparkContext, "../integration-tests/datasets/dicom_uncompressed")
      val metadaRowsCount = dicom.metadata.rowCount()
      val pixeldataRowsCount = dicom.pixeldata.rowCount()

      metadaRowsCount shouldBe 3
      pixeldataRowsCount shouldBe 3

      val keywordsValuesMap = Map(("SOPInstanceUID", "1.3.12.2.1107.5.2.5.11090.5.0.5823667428974336"), ("Manufacturer", "SIEMENS"), ("StudyDate", "20040305"))

      dicom.dropRowsByKeywords(keywordsValuesMap)

      val newMetadaRowsCount = dicom.metadata.rowCount()
      val newPixeldataRowsCount = dicom.pixeldata.rowCount()

      newMetadaRowsCount shouldBe 2
      newPixeldataRowsCount shouldBe 2

    }
  }
}