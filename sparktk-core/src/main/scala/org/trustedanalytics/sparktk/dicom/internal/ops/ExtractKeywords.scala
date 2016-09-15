package org.trustedanalytics.sparktk.dicom.internal.ops

import org.apache.spark.sql.Row
import org.trustedanalytics.sparktk.dicom.internal.{ BaseDicom, DicomTransform, DicomState }
import org.trustedanalytics.sparktk.frame.internal.rdd.RowWrapperFunctions
import org.trustedanalytics.sparktk.frame._
import org.trustedanalytics.sparktk.frame.internal._

trait ExtractKeywordsTransform extends BaseDicom {

  def extractKeywords(keywords: Seq[String]) = {
    execute(ExtractKeywords(keywords))
  }
}

case class ExtractKeywords(keywords: Seq[String]) extends DicomTransform {

  override def work(state: DicomState): DicomState = {
    ExtractKeywords.extractKeywordsImpl(state.metadata, keywords)
    state
  }
}

object ExtractKeywords extends Serializable {

  private implicit def rowWrapperToRowWrapperFunctions(rowWrapper: RowWrapper): RowWrapperFunctions = {
    new RowWrapperFunctions(rowWrapper)
  }

  /**
   * RowWrapper to apply on each row
   *
   * @param keywords keywords to add as columns
   * @return Row
   */
  private def customDicomAttributeRowWrapper(keywords: Seq[String]) = {
    val rowMapper: RowWrapper => Row = row => {
      val columnName = "metadata" //This should be column name of xml string in a frame
      val nodeName = "DicomAttribute" //This should be node name in xml string

      //Extracts the give column value and generates NodeSeq of DicomAttribute
      val nodeSeqOfDicomAttribute = row.valueAsNodeSeq(columnName, nodeName)

      //Filter each DicomAttribute node with given keyword and assign value to column
      val nodeValues = keywords.map(keyword => nodeSeqOfDicomAttribute.filter(dc => dc.attributes.get("keyword").get.toString == keyword).take(1)(0).text)

      //Creates a Row from given Sequence of node values
      Row.fromSeq(nodeValues)
    }
    rowMapper
  }

  /**
   * extracts the keyword values from xml and assigns to column
   *
   * @param metadataFrame metadata frame with column holding xml string
   * @param keywords keywords to extract from column holding xml string
   */
  def extractKeywordsImpl(metadataFrame: Frame, keywords: Seq[String]) = {
    val newColumns = for (keyword <- keywords) yield Column(keyword, DataTypes.string)
    metadataFrame.addColumns(customDicomAttributeRowWrapper(keywords), newColumns)
  }

}
