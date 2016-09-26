package org.trustedanalytics.sparktk.dicom.internal.ops

import java.io.File
import java.net.URI

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.{ FsAction, FsPermission }
import org.apache.hadoop.fs.{ Path, FileSystem }
import org.apache.spark.mllib.linalg.DenseMatrix
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.dcm4che3.data.{ VR, Tag }
import org.dcm4che3.io.DicomOutputStream
import org.dcm4che3.tool.xml2dcm.Xml2Dcm
import org.trustedanalytics.sparktk.dicom.internal.{ DicomState, DicomSummarization, BaseDicom }

trait ExportToDcmSummarization extends BaseDicom {
  /**
   * Export .dcm files to given hdfs path.
   *
   * Export the dicom images to hadoop path
   *
   * @param path The HDFS folder path where the files will be created.
   */
  def exportToDcm(path: String) = {
    execute(ExportToDcm(path))
  }
}

case class ExportToDcm(path: String) extends DicomSummarization[Unit] {

  override def work(state: DicomState): Unit = {
    ExportToDcm.exportToDcmFile(state.metadata.rdd, state.pixeldata.rdd, path)
  }
}

object ExportToDcm {

  def exportToDcmFile(metadataRdd: RDD[Row], pixeldataRdd: RDD[Row], path: String) = {

    val zipMetadataPixelData = metadataRdd.zip(pixeldataRdd)

    zipMetadataPixelData.cache()

    zipMetadataPixelData.foreach {
      case (metadata, pixeldata) => {

        val oneMetadata = metadata(1).toString
        var onePixeldata: DenseMatrix = pixeldata(1).asInstanceOf[DenseMatrix]

        val tmpFile: File = File.createTempFile(s"dicom-xml-temp", ".xml")
        FileUtils.writeByteArrayToFile(tmpFile, oneMetadata.getBytes)
        tmpFile.deleteOnExit()

        val dcmAttributes = Xml2Dcm.parseXML(tmpFile.getAbsolutePath)

        onePixeldata = onePixeldata.transpose
        val pixel = onePixeldata.toArray.map(x => x.toInt)
        dcmAttributes.setInt(Tag.PixelData, VR.OW, pixel: _*) //inserting modified pixeldata
        dcmAttributes.setInt(Tag.Rows, VR.US, onePixeldata.numRows)
        dcmAttributes.setInt(Tag.Columns, VR.US, onePixeldata.numCols)

        //write to /tmp directory and from there copy to hdfs
        val exportFile: File = File.createTempFile(s"export", ".dcm")
        exportFile.deleteOnExit()
        val dos: DicomOutputStream = new DicomOutputStream(exportFile)
        dcmAttributes.writeTo(dos)

        //copy to given path from /tmp
        val dcmTargetFileName = path + "/" + exportFile.getName
        val hdfsPath = new Path(dcmTargetFileName)
        val hdfsFileSystem = FileSystem.get(new URI(dcmTargetFileName), new Configuration())
        val localTmpPath = new Path(exportFile.getAbsolutePath)
        hdfsFileSystem.copyFromLocalFile(false, true, localTmpPath, hdfsPath)
        hdfsFileSystem.setPermission(hdfsPath, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.NONE))
      }
    }

  }
}