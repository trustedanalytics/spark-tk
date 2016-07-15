package org.trustedanalytics.sparktk.frame.internal.ops

import java.io.ByteArrayInputStream
import org.dcm4che3.io.DicomInputStream
import org.scalatest.Matchers
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class DicomTest extends TestingSparkContextWordSpec with Matchers {

  "dicom folder" should {
    "contain 9 files" in {

      //dicomimages folder is within spark-rk/core
      val dicomRdd = sparkContext.wholeTextFiles("dicomimages")
      val printbytes = dicomRdd.map { case (filepath, filedata) => filedata.getBytes().length }
      printbytes.foreach(println)

      val disRdd = dicomRdd.map { case (filepath, filedata) => new DicomInputStream(new ByteArrayInputStream(filedata.getBytes())) }
      val disDataRdd = disRdd.map { dis => dis.readDataset(1, 1) }
      disDataRdd.foreach(println)
    }
  }

}
