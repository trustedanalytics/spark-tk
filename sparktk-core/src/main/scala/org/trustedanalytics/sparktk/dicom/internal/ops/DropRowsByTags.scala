package org.trustedanalytics.sparktk.dicom.internal.ops

import org.apache.spark.sql.Row
import org.trustedanalytics.sparktk.dicom.Dicom
import org.trustedanalytics.sparktk.dicom.internal.{ BaseDicom, DicomTransform, DicomState }
import org.trustedanalytics.sparktk.frame.internal.rdd.RowWrapperFunctions
import org.trustedanalytics.sparktk.frame._
import org.trustedanalytics.sparktk.frame.internal._

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
    DropRowsByTags.dropRowsByTagsImpl(state.metadata, tagsValuesMap)
    val filteredIdFrame = state.metadata.copy(Some(Map("id" -> "id")))
    val filteredPixeldata = filteredIdFrame.joinInner(state.pixeldata, List("id"))
    DicomState(state.metadata, filteredPixeldata)
  }
}

object DropRowsByTags extends Serializable {

  //create RowWrapper from Row to access valueAsXmlNodeSeq method
  private implicit def rowWrapperToRowWrapperFunctions(row: Row)(implicit schema: Schema): RowWrapperFunctions = {
    val rowWrapper = new RowWrapper(schema).apply(row)
    new RowWrapperFunctions(rowWrapper)
  }

  //custom drop based on given tagsValuesMap
  def customTagsDrop(tagsValuesMap: Map[String, String])(row: Row)(implicit schema: Schema): Boolean = {

    //Creates NodeSeq of DicomAttribute
    val nodeSeqOfDicomAttribute = row.valueAsXmlNodeSeq(Dicom.metadataColumnName, Dicom.nodeNameInMetadata)

    tagsValuesMap.map {
      case (tag, value) => nodeSeqOfDicomAttribute.filter {
        dicomAttribute => (dicomAttribute \ "@tag").text == tag
      }.map { ns =>
        if (ns.nonEmpty)
          ns.head.text
        else
          null
      }.contains(value)
    }.reduce((a, b) => a && b)
  }

  /**
   * Drop the rows based on Map(tag, value) from column holding xml string
   *
   * @param metadataFrame metadata frame with column holding xml string
   * @param tagsValuesMap  Map with tag and associated value from xml string
   */
  def dropRowsByTagsImpl(metadataFrame: Frame, tagsValuesMap: Map[String, String]) = {
    implicit val schema = metadataFrame.schema
    metadataFrame.dropRows(customTagsDrop(tagsValuesMap))
  }

}