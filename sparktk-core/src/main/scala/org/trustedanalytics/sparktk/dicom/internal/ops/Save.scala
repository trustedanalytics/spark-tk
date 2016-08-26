package org.trustedanalytics.sparktk.dicom.internal.ops

import org.trustedanalytics.sparktk.dicom.DicomFrame
import org.trustedanalytics.sparktk.dicom.internal.{ DicomFrameState, DicomFrameSummarization, BaseDicomFrame }
import org.trustedanalytics.sparktk.saveload.TkSaveLoad

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