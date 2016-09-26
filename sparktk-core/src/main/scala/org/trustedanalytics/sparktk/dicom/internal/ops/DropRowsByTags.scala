package org.trustedanalytics.sparktk.dicom.internal.ops

import org.trustedanalytics.sparktk.dicom.internal.{ BaseDicom, DicomTransform, DicomState }

trait DropRowsByTagsTransform extends BaseDicom {

  /**
   * Drop the rows based on Map(tag, value) from column holding xml string
   *
   * @param tagsValuesMap Map with tag and associated value from xml string
   */
  def dropRowsByTags(tagsValuesMap: Map[String, String]) = {
    execute(DropRowsByTags(tagsValuesMap))
  }
}

case class DropRowsByTags(tagsValuesMap: Map[String, String]) extends DicomTransform {

  override def work(state: DicomState): DicomState = {
    FilterByTags.filterOrDropByTagsImpl(state.metadata, tagsValuesMap, isDropRows = true)
    val filteredIdFrame = state.metadata.copy(Some(Map("id" -> "id")))
    val filteredPixeldata = filteredIdFrame.joinInner(state.pixeldata, List("id"))
    DicomState(state.metadata, filteredPixeldata)
  }
}
