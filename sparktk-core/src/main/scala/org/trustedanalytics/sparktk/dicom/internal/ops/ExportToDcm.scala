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

import java.io.{ ByteArrayOutputStream, ByteArrayInputStream }
import java.net.URI
import javax.xml.parsers.{ SAXParser, SAXParserFactory }

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.{ FsAction, FsPermission }
import org.apache.hadoop.fs.{ FSDataOutputStream, Path, FileSystem }
import org.apache.spark.mllib.linalg.DenseMatrix
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.dcm4che3.data.{ UID, Attributes, VR, Tag }
import org.dcm4che3.io.{ ContentHandlerAdapter, DicomOutputStream }
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

object ExportToDcm extends Serializable {

  def getAttrFromMetadata(metadata: String): org.dcm4che3.data.Attributes = {
    val attrs: Attributes = new Attributes
    val ch: ContentHandlerAdapter = new ContentHandlerAdapter(attrs)
    val f: SAXParserFactory = SAXParserFactory.newInstance
    val p: SAXParser = f.newSAXParser

    val metadataByteStream = new ByteArrayInputStream(metadata.getBytes)
    p.parse(metadataByteStream, ch)
    return attrs
  }

  def writeToHdfs(dcmAttributes: Attributes, path: String): Unit = {
    val b = new ByteArrayOutputStream()
    val d = new DicomOutputStream(b, UID.ExplicitVRLittleEndian)
    dcmAttributes.writeTo(d)

    val bytes = b.toByteArray

    val fileName = s"${java.lang.System.currentTimeMillis()}.dcm"
    val filePath = StringUtils.join(path, "/", fileName)

    val hdfsPath = new Path(filePath)
    val fs = FileSystem.get(new Configuration())
    val hdfsOutputStream = fs.create(hdfsPath)
    hdfsOutputStream.write(bytes)
    hdfsOutputStream.close()

  }

  def exportToDcmFile(metadataRdd: RDD[Row], pixeldataRdd: RDD[Row], path: String) = {

    val zipMetadataPixelData = metadataRdd.zip(pixeldataRdd)

    zipMetadataPixelData.cache()

    val attrs = zipMetadataPixelData.foreach {
      case (metadata, pixeldata) => {

        val oneMetadata = metadata(1).toString
        val onePixeldata: DenseMatrix = pixeldata(1).asInstanceOf[DenseMatrix]

        val dcmAttributes = getAttrFromMetadata(oneMetadata)

        val pixel = onePixeldata.toArray.map(x => x.toInt)
        dcmAttributes.setInt(Tag.PixelData, VR.OW, pixel: _*)
        dcmAttributes.setInt(Tag.Rows, VR.US, onePixeldata.numRows)
        dcmAttributes.setInt(Tag.Columns, VR.US, onePixeldata.numCols)

        writeToHdfs(dcmAttributes, path)
      }
    }

  }
}