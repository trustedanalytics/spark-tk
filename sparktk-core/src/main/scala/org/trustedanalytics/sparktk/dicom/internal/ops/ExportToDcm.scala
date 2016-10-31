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
import javax.xml.parsers.{ SAXParser, SAXParserFactory }

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ Path, FileSystem }
import org.apache.spark.mllib.linalg.DenseMatrix
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.dcm4che3.data.{ UID, Attributes, VR, Tag }
import org.dcm4che3.io.{ ContentHandlerAdapter, DicomOutputStream }
import org.trustedanalytics.sparktk.dicom.internal.{ DicomState, DicomSummarization, BaseDicom }

trait ExportToDcmSummarization extends BaseDicom {
  /**
   * Export .dcm files to given Local/HDFS path.
   *
   * @param path Local/HDFS path to export dicom files
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

  /**
   * Method to generate DicomAttributes from given metadata xml string
   *
   * @param metadata metadata xml string
   * @return Attributes
   */
  def getAttributesFromMetadata(metadata: String): Attributes = {
    val attrs: Attributes = new Attributes
    val ch: ContentHandlerAdapter = new ContentHandlerAdapter(attrs)
    val saxFactory: SAXParserFactory = SAXParserFactory.newInstance
    val saxParser: SAXParser = saxFactory.newSAXParser

    //Create ByteArrayInputStream from metadata string
    val metadataByteStream = new ByteArrayInputStream(metadata.getBytes)
    saxParser.parse(metadataByteStream, ch)
    attrs
  }

  /**
   * Method to write DicomAttributes to given path as .dcm files
   *
   * @param dcmAttributes DicomAttributes
   * @param path Local/HDFS path
   */
  def writeToHDFS(id: String, dcmAttributes: Attributes, path: String): Unit = {
    val byteOutputStream = new ByteArrayOutputStream()
    //UID.ExplicitVRLittleEndian will enforce to include datatypes(also known as VR-Value Representation) for Attributes in Dicom File
    val dicomOutputStream = new DicomOutputStream(byteOutputStream, UID.ExplicitVRLittleEndian)
    dcmAttributes.writeTo(dicomOutputStream)

    //bytes to write to hdfs
    val bytes = byteOutputStream.toByteArray

    val fileName = s"$id.dcm"
    val filePath = StringUtils.join(path, "/", fileName)

    val hdfsPath = new Path(filePath)
    val fs = FileSystem.get(new Configuration())
    val hdfsOutputStream = fs.create(hdfsPath, false)
    hdfsOutputStream.write(bytes)
    hdfsOutputStream.close()
  }

  def exportToDcmFile(metadataRdd: RDD[Row], pixeldataRdd: RDD[Row], path: String) = {

    //zip to join back (metadata, pixeldata into one)
    val zipMetadataPixeldata = metadataRdd.zip(pixeldataRdd)

    //zips metadataRDD [(id, metadata_columns)] and pixeldataRDD[(id, pixeldata)]
    zipMetadataPixeldata.foreach {
      case (metadata, pixeldata) => {

        val id = metadata(0).toString
        //Access the metadata column from metadatardd row
        val metadataStr = metadata(1).toString
        //Access the pixeldata column from pixeldataRdd row
        //Mllib DenseMatrix is column-major, it fills the values column-wise, so matrix looks transposed.
        //To revert back to original we are transposing it when casting.
        val pixeldataDM: DenseMatrix = pixeldata(1).asInstanceOf[DenseMatrix].transpose

        val dcmAttributes = getAttributesFromMetadata(metadataStr)
        val pixel = pixeldataDM.toArray.map(_.toInt)

        //Set modified pixeldata to DicomAttributes
        dcmAttributes.setInt(Tag.PixelData, VR.OW, pixel: _*)
        dcmAttributes.setInt(Tag.Rows, VR.US, pixeldataDM.numRows)
        dcmAttributes.setInt(Tag.Columns, VR.US, pixeldataDM.numCols)

        //write updated DicomAttributes to Local/HDFS as .dcm files
        writeToHDFS(id, dcmAttributes, path)
      }
    }

  }
}