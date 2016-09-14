package org.trustedanalytics.sparktk.dicom.internal.constructors

import java.awt.image.Raster
import java.io.{ PrintStream, ByteArrayOutputStream, File }
import java.util.Iterator
import javax.imageio.stream.ImageInputStream
import javax.imageio.{ ImageIO, ImageReader }

import org.apache.commons.io.FileUtils
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.DenseMatrix
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.trustedanalytics.sparktk.dicom.Dicom
import org.trustedanalytics.sparktk.frame.Frame
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd

import org.dcm4che3.imageio.plugins.dcm.{ DicomImageReadParam, DicomImageReader }
import org.dcm4che3.io.DicomInputStream
import org.dcm4che3.tool.dcm2xml.Dcm2Xml

import scala.util.Random

object Import {
  /**
   * Creates a dicom object with metadata and pixeldata frames
   *
   * @param path Full path to the DICOM files directory
   * @return Dicom object with MetadataFrame and PixeldataFrame
   */
  def importDcm(sc: SparkContext, path: String): Dicom = {

    val dicomFilesRdd = sc.binaryFiles(path)

    val dcmMetadataPixelArrayRDD = dicomFilesRdd.map {

      case (filePath, fileData) =>

        //TODO: create .dcm files in /tmp and create file Obj. Currently dicom library does not support byte arrays (Temporary)
        val tmpFile: File = File.createTempFile(s"dicom-temp-${Random.nextInt()}", ".dcm")
        FileUtils.writeByteArrayToFile(tmpFile, fileData.toArray())
        tmpFile.deleteOnExit()

        //create matrix
        val iter: Iterator[ImageReader] = ImageIO.getImageReadersByFormatName("DICOM")
        val readers: DicomImageReader = iter.next.asInstanceOf[DicomImageReader]
        val param: DicomImageReadParam = readers.getDefaultReadParam.asInstanceOf[DicomImageReadParam]
        val iis: ImageInputStream = ImageIO.createImageInputStream(tmpFile)
        readers.setInput(iis, true)

        //pixels data raster
        val raster: Raster = readers.readRaster(0, param)

        val w = raster.getWidth
        val h = raster.getHeight

        val data = Array.ofDim[Double](h, w)

        for {
          i <- 0 until h
          j <- 0 until w
        } data(i)(j) = raster.getSample(i, j, 0)

        //Create a dense matrix for pixel array
        val dm1 = new DenseMatrix(h, w, data.flatten)

        //Metadata
        val dis: DicomInputStream = new DicomInputStream(tmpFile)
        val dcm2xml: Dcm2Xml = new Dcm2Xml()

        //TODO: Fix the redirecting output stream (Temporary)
        //redirecting output stream
        val myOutputStream = new ByteArrayOutputStream()
        val myStream: PrintStream = new PrintStream(myOutputStream)
        // Listen to system out
        System.setOut(myStream)
        dcm2xml.parse(dis)
        // Restore (or stop listening)
        System.out.flush()
        System.setOut(System.out)

        // myStream.toString
        val xml: String = myOutputStream.toString()
        (xml, dm1)
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
