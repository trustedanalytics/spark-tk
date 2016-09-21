package org.trustedanalytics.sparktk.dicom.internal.ops

import org.apache.spark.sql.Row
import org.trustedanalytics.sparktk.dicom.Dicom
import org.trustedanalytics.sparktk.dicom.internal.{ BaseDicom, DicomTransform, DicomState }
import org.trustedanalytics.sparktk.frame.internal.rdd.RowWrapperFunctions
import org.trustedanalytics.sparktk.frame._
import org.trustedanalytics.sparktk.frame.internal._

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
    DropRowsByKeywords.dropRowsByKeywordsImpl(state.metadata, keywordsValuesMap)
    val filteredIdFrame = state.metadata.copy(Some(Map("id" -> "id")))
    val filteredPixeldata = filteredIdFrame.joinInner(state.pixeldata, List("id"))
    DicomState(state.metadata, filteredPixeldata)
  }
}

object DropRowsByKeywords extends Serializable {

  //create RowWrapper from Row to access valueAsXmlNodeSeq method
  private implicit def rowWrapperToRowWrapperFunctions(row: Row)(implicit schema: Schema): RowWrapperFunctions = {
    val rowWrapper = new RowWrapper(schema).apply(row)
    new RowWrapperFunctions(rowWrapper)
  }

  //custom drop based on given keywordsValuesMap
  def customKeywordsDrop(keywordsValuesMap: Map[String, String])(row: Row)(implicit schema: Schema): Boolean = {

    //Creates NodeSeq of DicomAttribute
    val nodeSeqOfDicomAttribute = row.valueAsXmlNodeSeq(Dicom.metadataColumnName, Dicom.nodeNameInMetadata)

    keywordsValuesMap.map {
      case (keyword, value) => nodeSeqOfDicomAttribute.filter {
        dicomAttribute => (dicomAttribute \ "@keyword").text == keyword
      }.map { ns =>
        if (ns.nonEmpty)
          ns.head.text
        else
          null
      }.contains(value)
    }.reduce((a, b) => a && b)
  }

  /**
    * Drop the rows based on Map(keyword, value) from column holding xml string
    *
    * @param metadataFrame metadata frame with column holding xml string
    * @param keywordsValuesMap  Map with keyword and associated value from xml string
    */
  def dropRowsByKeywordsImpl(metadataFrame: Frame, keywordsValuesMap: Map[String, String]) = {
    implicit val schema = metadataFrame.schema
    metadataFrame.dropRows(customKeywordsDrop(keywordsValuesMap))
  }

}