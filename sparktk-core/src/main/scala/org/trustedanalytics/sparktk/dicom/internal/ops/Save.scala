package org.trustedanalytics.sparktk.dicom.internal.ops

import org.trustedanalytics.sparktk.dicom.Dicom
import org.trustedanalytics.sparktk.dicom.internal.{ DicomState, DicomSummarization, BaseDicom }
import org.trustedanalytics.sparktk.saveload.TkSaveLoad

trait SaveSummarization extends BaseDicom {
  /**
   * Save the current frame.
   *
   * @param path The destination path.
   */
  def save(path: String): Unit = {
    execute(Save(path))
  }
}

case class Save(path: String) extends DicomSummarization[Unit] {

  override def work(state: DicomState): Unit = {
    state.dicomFrame.metadata.dataframe.write.parquet(path + "/metadata")
    state.dicomFrame.imagedata.dataframe.write.parquet(path + "/imagedata")
    val formatId = Dicom.formatId
    val formatVersion = Dicom.tkFormatVersion
    TkSaveLoad.saveTk(state.dicomFrame.metadata.dataframe.sqlContext.sparkContext, path, formatId, formatVersion, "No Metadata")
  }
}