package org.trustedanalytics.sparktk.dicom.internal.constructors

import java.awt.image.Raster
import java.io.{ PrintStream, ByteArrayOutputStream, File }
import java.util.Iterator
import javax.imageio.stream.ImageInputStream
import javax.imageio.{ ImageIO, ImageReader }

import org.apache.commons.io.FileUtils
import org.apache.commons.lang.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.DenseMatrix
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.dcm4che3.imageio.plugins.dcm.{ DicomImageReadParam, DicomImageReader }
import org.dcm4che3.io.DicomInputStream
import org.dcm4che3.tool.dcm2xml.Dcm2Xml
import org.trustedanalytics.sparktk.dicom.DicomFrame
import org.trustedanalytics.sparktk.frame._
import org.apache.spark.sql.types.{ StructType, StructField }
import org.trustedanalytics.sparktk.frame.{ DataTypes, Schema, Frame }
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd

import scala.util.Random

object Import {
  /**
   * Creates a dicom frame by importing .dcm files from directory
   *
   * @param path Full path to the DICOM files directory
   * @return DicomFrame case class with MetadataFrame and ImageDataFrame
   */
  def importDicom(sc: SparkContext, path: String): DicomFrame = {

    //val mypath="hdfs://10.7.151.97:8020/user/kvadla/dicom_images_decompressed"
    val dicomFilesRdd = sc.binaryFiles(path)

    val dcmMetadataPixelArrayRDD = dicomFilesRdd.map {

      case (filePath, fileData) =>

        //create .dcm files in /tmp and create file Obj. Currently reading bytes from hdfs is not supported
        val tmpFile = File.createTempFile(s"dicom-test-${Random.nextInt()}", ".dcm")
        FileUtils.writeByteArrayToFile(tmpFile, fileData.toArray())

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

        //dicom pixel array
        val pixel_data_array: Array[Double] = data.flatten
        //Create a dense matrix for pixel array
        val dm1 = new DenseMatrix(h, w, pixel_data_array)

        //Metadata
        val dis: DicomInputStream = new DicomInputStream(tmpFile)
        val dcm2xml: Dcm2Xml = new Dcm2Xml()

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
    }.zipWithUniqueId()

    //create metadata pairrdd
    val metaDataPairRDD: RDD[(Long, String)] = dcmMetadataPixelArrayRDD.map(row => (row._2, row._1._1))

    //create image matrix pair rdd
    val imageMatrixPairRDD: RDD[(Long, DenseMatrix)] = dcmMetadataPixelArrayRDD.map(row => (row._2, row._1._2))

    val sqlCtx = new SQLContext(sc)
    import sqlCtx.implicits._

    val metadataDF = metaDataPairRDD.toDF("id", "metadata")
    val metadataFrameRdd = FrameRdd.toFrameRdd(metadataDF)
    val metadataFrame = new Frame(metadataFrameRdd, metadataFrameRdd.frameSchema)

    val imageDF = imageMatrixPairRDD.toDF("id", "imagematrix")
    val imagedataFrameRdd = FrameRdd.toFrameRdd(imageDF)
    val imagedataFrame = new Frame(imagedataFrameRdd, imagedataFrameRdd.frameSchema)

    new DicomFrame(metadataFrame, imagedataFrame)
  }

}
