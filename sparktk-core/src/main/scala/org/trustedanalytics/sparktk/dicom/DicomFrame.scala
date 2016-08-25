package org.trustedanalytics.sparktk.dicom

import org.apache.spark.SparkContext
import org.json4s.JsonAST.JValue
import org.trustedanalytics.sparktk.dicom.internal.constructors.{ DicomFrameState, DicomFrameSummarization, BaseDicomFrame }
import org.trustedanalytics.sparktk.frame.Frame
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.trustedanalytics.sparktk.saveload.{ TkSaveLoad, TkSaveableObject }

class DicomFrame(val metadataFrame: Frame, val imagedataFrame: Frame) extends BaseDicomFrame with Serializable with SaveSummarization

object DicomFrame extends TkSaveableObject {

  val tkFormatVersion = 1

  /**
   * Loads the parquet files (the vertices and edges dataframes) found at the given path and returns a Graph
   *
   * @param sc active SparkContext
   * @param path path to the file
   * @param formatVersion TK metadata formatVersion
   * @param tkMetadata TK metadata
   * @return
   */
  def loadTkSaveableObject(sc: SparkContext, path: String, formatVersion: Int = tkFormatVersion, tkMetadata: JValue = null): Any = {
    require(tkFormatVersion == formatVersion, s"Graph load only supports version $tkFormatVersion.  Got version $formatVersion")
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

trait SaveSummarization extends BaseDicomFrame {
  /**
   * Save the current frame.
   *
   * @param path The destination path.
   */
  def save(path: String): Unit = {
    execute(Save(path))
  }
}

case class Save(path: String) extends DicomFrameSummarization[Unit] {

  override def work(state: DicomFrameState): Unit = {
    state.dicomFrame.metadataFrame.dataframe.write.parquet(path + "/metadata")
    state.dicomFrame.imagedataFrame.dataframe.write.parquet(path + "/imagedata")
    val formatId = DicomFrame.formatId
    val formatVersion = DicomFrame.tkFormatVersion
    TkSaveLoad.saveTk(state.dicomFrame.metadataFrame.dataframe.sqlContext.sparkContext, path, formatId, formatVersion, "No Metadata")
  }
}
