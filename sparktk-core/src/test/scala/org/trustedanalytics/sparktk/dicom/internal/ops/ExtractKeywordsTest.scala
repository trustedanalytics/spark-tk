package org.trustedanalytics.sparktk.dicom.internal.ops

import org.scalatest.Matchers
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec
import org.trustedanalytics.sparktk.dicom.internal.constructors.Import.importDcm

class ExtractKeywordsTest extends TestingSparkContextWordSpec with Matchers {

  "Extract Keywords" should {
    "extract value for each keyword and add column for each keyword to assign value. For missing keyword, value is null" in {
      val dicom = importDcm(sparkContext, "../integration-tests/datasets/dicom_uncompressed")
      val columnCount = dicom.metadata.schema.columnNames.length

      columnCount shouldBe 2

      val keywords = Seq("SOPInstanceUID", "Manufacturer", "StudyDate")

      dicom.extractKeywords(keywords)

      val newColumnCount = dicom.metadata.schema.columnNames.length

      newColumnCount shouldBe 5

      val expectedColNames = Seq("id", "metadata", "SOPInstanceUID", "Manufacturer", "StudyDate")

      val actualColNames = dicom.metadata.schema.columnNames

      actualColNames should contain theSameElementsAs (expectedColNames)
    }
  }
}
