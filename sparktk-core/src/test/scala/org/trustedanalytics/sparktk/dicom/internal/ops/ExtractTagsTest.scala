package org.trustedanalytics.sparktk.dicom.internal.ops

import org.scalatest.Matchers
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec
import org.trustedanalytics.sparktk.dicom.internal.constructors.Import.importDcm

class ExtractTagsTest extends TestingSparkContextWordSpec with Matchers {

  "Extract Tags" should {
    "extract value for each tag and add column for each tag to assign value. For missing tag, value is null" in {
      val dicom = importDcm(sparkContext, "../integration-tests/datasets/dicom_uncompressed")
      val columnCount = dicom.metadata.schema.columnNames.length

      columnCount shouldBe 2

      val tags = Seq("00080018", "00080070", "00080020")

      dicom.extractTags(tags)

      val newColumnCount = dicom.metadata.schema.columnNames.length

      newColumnCount shouldBe 5

      val expectedColNames = Seq("id", "metadata", "00080018", "00080070", "00080020")

      val actualColNames = dicom.metadata.schema.columnNames

      actualColNames should contain theSameElementsAs (expectedColNames)
    }
  }
}
