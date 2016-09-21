package org.trustedanalytics.sparktk.dicom.internal.ops

import org.scalatest.Matchers
import org.trustedanalytics.sparktk.dicom.internal.constructors.Import._
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class FilterByTagsTest extends TestingSparkContextWordSpec with Matchers {

  "FilterByTags" should {
    "Filter the rows based on Map(tag, value) from column holding xml string" in {
      val dicom = importDcm(sparkContext, "../integration-tests/datasets/dicom_uncompressed")
      val metadaRowsCount = dicom.metadata.rowCount()
      val pixeldataRowsCount = dicom.pixeldata.rowCount()

      metadaRowsCount shouldBe 3
      pixeldataRowsCount shouldBe 3

      val tagsValuesMap = Map(("00080018", "1.3.12.2.1107.5.2.5.11090.5.0.5823667428974336"), ("00080070", "SIEMENS"), ("00080020", "20040305"))

      dicom.filterByTags(tagsValuesMap)

      val newMetadaRowsCount = dicom.metadata.rowCount()
      val newPixeldataRowsCount = dicom.pixeldata.rowCount()

      newMetadaRowsCount shouldBe 1
      newPixeldataRowsCount shouldBe 1

    }
  }
}