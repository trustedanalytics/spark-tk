package org.trustedanalytics.sparktk.dicom

import org.apache.spark.SparkContext
import org.json4s.JsonAST.JValue
import org.trustedanalytics.sparktk.dicom.internal.BaseDicomFrame
import org.trustedanalytics.sparktk.dicom.internal.ops.SaveSummarization
import org.trustedanalytics.sparktk.frame.Frame
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.trustedanalytics.sparktk.saveload.TkSaveableObject

class DicomFrame(val metadataFrame: Frame, val imagedataFrame: Frame) extends BaseDicomFrame with Serializable with SaveSummarization

object DicomFrame extends TkSaveableObject {

  val tkFormatVersion = 1

  /**
   * Loads the parquet files (the metadata and imagedata dataframes) found at the given path and returns a DicomFrame
   *
   * @param sc active SparkContext
   * @param path path to the file
   * @param formatVersion TK metadata formatVersion
   * @param tkMetadata TK metadata
   * @return
   */
  def loadTkSaveableObject(sc: SparkContext, path: String, formatVersion: Int = tkFormatVersion, tkMetadata: JValue = null): Any = {
    require(tkFormatVersion == formatVersion, s"DicomFrame load only supports version $tkFormatVersion.  Got version $formatVersion")
    // no extra metadata in version 1
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val metadataDF = sqlContext.read.parquet(path + "/metadata")
    val imageDF = sqlContext.read.parquet(path + "/imagedata")

    val metadataFrameRdd = FrameRdd.toFrameRdd(metadataDF)
    val metadataFrame = new Frame(metadataFrameRdd, metadataFrameRdd.frameSchema)

    val imagedataFrameRdd = FrameRdd.toFrameRdd(imageDF)
    val imagedataFrame = new Frame(imagedataFrameRdd, imagedataFrameRdd.frameSchema)

    new DicomFrame(metadataFrame, imagedataFrame)
  }
}

