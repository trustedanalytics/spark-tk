package org.trustedanalytics.sparktk.dicom.internal.ops

import org.trustedanalytics.sparktk.dicom.internal.{ BaseDicom, DicomTransform, DicomState }

trait DropRowsByKeywordsTransform extends BaseDicom {

  /**
   * Drop the rows based on Map(keyword, value) from column holding xml string
   *
   * @param keywordsValuesMap Map with keyword and associated value from xml string
   */
  def dropRowsByKeywords(keywordsValuesMap: Map[String, String]) = {
    execute(DropRowsByKeywords(keywordsValuesMap))
  }
}

case class DropRowsByKeywords(keywordsValuesMap: Map[String, String]) extends DicomTransform {

  override def work(state: DicomState): DicomState = {
    FilterByKeywords.filterOrDropByKeywordsImpl(state.metadata, keywordsValuesMap, isDropRows = true)
    val filteredIdFrame = state.metadata.copy(Some(Map("id" -> "id")))
    val filteredPixeldata = filteredIdFrame.joinInner(state.pixeldata, List("id"))
    DicomState(state.metadata, filteredPixeldata)
  }
}