package org.trustedanalytics.sparktk.dicom

import java.awt.image.Raster
import java.io._
import java.util.Iterator
import javax.imageio.stream.ImageInputStream
import javax.imageio.{ImageIO, ImageReader}

import org.apache.commons.io.FileUtils
import org.apache.spark.mllib.linalg.DenseMatrix
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.dcm4che3.imageio.plugins.dcm.{DicomImageReadParam, DicomImageReader}
import org.dcm4che3.io.DicomInputStream
import org.dcm4che3.tool.dcm2xml.Dcm2Xml
import org.scalatest.Matchers
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

import scala.language.postfixOps
import scala.util.Random

class DicomHdfs extends TestingSparkContextWordSpec with Matchers {

  "dicom folder" should {
    "dicom files load and create dataframe test" in {

     val dicomFilesRdd = sparkContext.binaryFiles("hdfs://10.7.151.97:8020/user/kvadla/dicom_images_decompressed")

      var id =0

      val dcmMetadataPixelArrayRDD = dicomFilesRdd.map{

        case (filePath, fileData)  =>

          //val myInputStream: InputStream  = new ByteArrayInputStream(fileData.toArray())

          val tmpFile = File.createTempFile(s"dicom-test-${Random.nextInt()}", ".dcm")
          FileUtils.writeByteArrayToFile(tmpFile, fileData.toArray())

          //create matrix

          val iter: Iterator[ImageReader] = ImageIO.getImageReadersByFormatName("DICOM")

          val readers: DicomImageReader = iter.next.asInstanceOf[DicomImageReader]

          val param: DicomImageReadParam = readers.getDefaultReadParam.asInstanceOf[DicomImageReadParam]

          //	Adjust the values of Rows and Columns in it and add a Pixel Data attribute with
          // the byte array from the DataBuffer of the scaled Raster
          val iis: ImageInputStream = ImageIO.createImageInputStream(tmpFile)

          readers.setInput(iis, true)

          //pixels data raster
          val raster: Raster = readers.readRaster(0, param)

          val w = raster.getWidth
          val h = raster.getHeight

          val data = Array.ofDim[Double](w, h)

          for{
            i <- 0 until h
            j <- 0 until w
          } data(i)(j) = raster.getSample(i, j, 0)

          //dicom pixel array
          val pixel_data_array: Array[Double]= data.flatten


          //Create a dense matrix for pixel array
          val dm1 = new DenseMatrix(h, w, pixel_data_array)

          //Reading metadata attributes
          // val dis: DicomInputStream = new DicomInputStream(fileObj)
          // val fmi = dis.getFileMetaInformation
          // val metaData = dis.readDataset(-1, -1)

          val dis: DicomInputStream = new DicomInputStream(tmpFile)
          val dcm2xml:Dcm2Xml =new Dcm2Xml()

          id = id +1

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
          (id, xml, dm1)
      }

      dcmMetadataPixelArrayRDD.foreach(println)

      //create metadata pairrdd
      val metaDataPairRDD:RDD[(Int, String)] = dcmMetadataPixelArrayRDD.map(row => (row._1, row._2))
      //metaDataPairRDD.foreach(println)

      //create image matrix pair rdd
      val imageMatrixPairRDD:RDD[(Int, DenseMatrix)] = dcmMetadataPixelArrayRDD.map(row => (row._1, row._3))
      //imageMatrixPairRDD.foreach(println)


      val sqlCtx = new SQLContext(sparkContext)
      import sqlCtx.implicits._


      println("---------------Metadata----------------")
      val metadataDF= metaDataPairRDD.toDF("id", "metadata")
      //metadataDF.show(5)
      //println(metadataDF.printSchema())

      println("---------------Image matrix------------------")
      val imageDF = imageMatrixPairRDD.toDF("id", "imagematrix")
      //imageDF.show(5)
      //println(imageDF.printSchema())
    }
  }

}
