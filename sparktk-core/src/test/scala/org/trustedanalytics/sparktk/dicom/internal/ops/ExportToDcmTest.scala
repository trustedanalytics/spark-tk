package org.trustedanalytics.sparktk.dicom.internal.ops

import java.io._

import org.apache.commons.io.{ IOUtils, FileUtils }
import org.dcm4che3.data.{ VR, Tag }
import org.dcm4che3.io.DicomOutputStream
import org.scalatest.Matchers
import org.trustedanalytics.sparktk.dicom.internal.constructors.Import.importDcm
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec
import org.dcm4che3.tool.xml2dcm.Xml2Dcm
import org.apache.spark.mllib.linalg.DenseMatrix

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import scala.util.Random

class ExportToDcmTest extends TestingSparkContextWordSpec with Matchers {

  "ExportToDcm" should {
    "create new .dcm from metadata and pixeldata and export to specified hdfs/local path" in {
      val dicom = importDcm(sparkContext, "../integration-tests/datasets/dicom_uncompressed")
      val oneMetadata = dicom.metadata.take(1)(0)(1).toString

      println("Metadata")
      println(oneMetadata)

      var onePixeldata: DenseMatrix = dicom.pixeldata.take(1)(0)(1).asInstanceOf[DenseMatrix]

      println("Pixeldata")
      println(onePixeldata)

      val tmpFile: File = File.createTempFile(s"dicom-temp", ".xml")
      FileUtils.writeByteArrayToFile(tmpFile, oneMetadata.getBytes)

      val dcmAttrs = Xml2Dcm.parseXML(tmpFile.getAbsolutePath)

      onePixeldata = onePixeldata.transpose
      val pixel = onePixeldata.toArray.map(x => x.toInt)
      dcmAttrs.setInt(Tag.PixelData, VR.OW, pixel: _*)
      dcmAttrs.setInt(Tag.Rows, VR.US, onePixeldata.numRows)
      dcmAttrs.setInt(Tag.Columns, VR.US, onePixeldata.numCols)

      val exportFile: File = File.createTempFile(s"export", ".dcm")
      val dos: DicomOutputStream = new DicomOutputStream(exportFile)
      dcmAttrs.writeTo(dos)

      //write to hdfs

      val hdfsPath = "dicom_export/"
      //System.setProperty("HADOOP_USER_NAME", "kvadla")
      val conf = new Configuration()
      conf.set("fs.defaultFS", "hdfs://10.7.151.97:8020")
      val fs = FileSystem.get(conf)

      val destPath = new Path(hdfsPath + exportFile.getName)
      val srcPath = new Path(exportFile.getAbsolutePath)

      fs.copyFromLocalFile(srcPath, destPath)
    }
  }
}
