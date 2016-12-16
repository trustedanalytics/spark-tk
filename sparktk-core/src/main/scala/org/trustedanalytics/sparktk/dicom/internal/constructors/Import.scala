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
package org.trustedanalytics.sparktk.dicom.internal.constructors

import java.awt.image.Raster
import java.io._
import java.util.Iterator
import javax.imageio.stream.ImageInputStream
import javax.imageio.{ ImageIO, ImageReader }

import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.DenseMatrix
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.trustedanalytics.sparktk.dicom.Dicom
import org.trustedanalytics.sparktk.frame.Frame
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.dcm4che3.imageio.plugins.dcm.{ DicomImageReadParam, DicomImageReader }
import org.dcm4che3.io.DicomInputStream
import org.dcm4che3.io.DicomInputStream.IncludeBulkData
import org.dcm4che3.tool.dcm2xml.org.trustedanalytics.sparktk.Dcm2Xml

object Import extends Serializable {

  /**
   * Get Pixel Data from Dicom Input Stream represented as Array of Bytes
   *
   * @param byteArray Dicom Input Stream represented as Array of Bytes
   * @return DenseMatrix Pixel Data
   */
  def getPixeldata(byteArray: Array[Byte]): DenseMatrix = {

    val pixeldataInputStream = new DataInputStream(new ByteArrayInputStream(byteArray))
    val pixeldicomInputStream = new DicomInputStream(pixeldataInputStream)

    //create matrix
    val iter: Iterator[ImageReader] = ImageIO.getImageReadersByFormatName("DICOM")
    val readers: DicomImageReader = iter.next.asInstanceOf[DicomImageReader]
    val param: DicomImageReadParam = readers.getDefaultReadParam.asInstanceOf[DicomImageReadParam]
    val iis: ImageInputStream = ImageIO.createImageInputStream(pixeldicomInputStream)
    readers.setInput(iis, true)

    //pixels data raster
    val raster: Raster = readers.readRaster(0, param)

    val cols = raster.getWidth
    val rows = raster.getHeight

    val data = Array.ofDim[Double](cols, rows)

    // Reading pixeldata along with x-axis and y-axis(x, y) but storing the data in 2D array as column-major.
    // Mllib DenseMatrix stores data as column-major.
    for (x <- 0 until cols) {
      for (y <- 0 until rows) {
        data(x)(y) = raster.getSample(x, y, 0)
      }
    }
    new DenseMatrix(rows, cols, data.flatten)
  }

  /**
   * Get Metadata Xml from Dicom Input Stream represented as byte array
   *
   * @param byteArray Dicom Input Stream represented as byte array
   * @return String Xml Metadata
   */
  def getMetadataXml(byteArray: Array[Byte]): String = {
    val metadataInputStream = new DataInputStream(new ByteArrayInputStream(byteArray))
    val metadataDicomInputStream = new DicomInputStream(metadataInputStream)
    val dcm2xml = new Dcm2Xml()
    val myOutputStream = new ByteArrayOutputStream()
    dcm2xml.convert(metadataDicomInputStream, myOutputStream)
    myOutputStream.toString()
  }

  /**
   * Creates a dicom object with metadata and pixeldata frames
   *                                                                            |---> DataInputStream --> DicomInputStream --> Dcm2Xml --> Metadata XML (String)
   *                                                                            |
   * Spark foreach DCM Image (FilePath, PortableDataStream) ---> ByteArray --->
   *                                                                            |
   *                                                                            |---> DataInputStream --> DicomInputStream --> ImageInputStream --> Raster --> Pixel Data (Dense Matrix)
   *
   * @param path Full path to the DICOM files directory
   * @return Dicom object with MetadataFrame and PixeldataFrame
   */
  def importDcm(sc: SparkContext, path: String, minPartitions: Int = 2): Dicom = {

    val dicomFilesRdd = sc.binaryFiles(path, minPartitions)

    val dcmMetadataPixelArrayRDD = dicomFilesRdd.map {

      case (filePath, fileData) => {
        // Open PortableDataStream to retrieve the bytes
        val fileInputStream = fileData.open()
        val byteArray = IOUtils.toByteArray(fileInputStream)

        //Create the metadata xml
        val metadata = getMetadataXml(byteArray)
        //Create a dense matrix for pixel array
        val pixeldata = getPixeldata(byteArray)

        //Close the PortableDataStream
        fileInputStream.close()

        (metadata, pixeldata)
      }
    }.zipWithIndex()

    dcmMetadataPixelArrayRDD.cache()

    val sqlCtx = new SQLContext(sc)
    import sqlCtx.implicits._

    //create metadata pairrdd
    val metaDataPairRDD: RDD[(Long, String)] = dcmMetadataPixelArrayRDD.map {
      case (metadataPixeldata, id) => (id, metadataPixeldata._1)
    }

    val metadataDF = metaDataPairRDD.toDF("id", "metadata")
    val metadataFrameRdd = FrameRdd.toFrameRdd(metadataDF)
    val metadataFrame = new Frame(metadataFrameRdd, metadataFrameRdd.frameSchema)

    //create image matrix pair rdd
    val imageMatrixPairRDD: RDD[(Long, DenseMatrix)] = dcmMetadataPixelArrayRDD.map {
      case (metadataPixeldata, id) => (id, metadataPixeldata._2)
    }

    val imageDF = imageMatrixPairRDD.toDF("id", "imagematrix")
    val pixeldataFrameRdd = FrameRdd.toFrameRdd(imageDF)
    val pixeldataFrame = new Frame(pixeldataFrameRdd, pixeldataFrameRdd.frameSchema)

    new Dicom(metadataFrame, pixeldataFrame)
  }

}
