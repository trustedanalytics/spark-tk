package org.trustedanalytics.sparktk.dicom.internal.ops

import org.apache.spark.sql.Row
import org.trustedanalytics.sparktk.dicom.Dicom
import org.trustedanalytics.sparktk.dicom.internal.{ BaseDicom, DicomTransform, DicomState }
import org.trustedanalytics.sparktk.frame.internal.rdd.RowWrapperFunctions
import org.trustedanalytics.sparktk.frame._
import org.trustedanalytics.sparktk.frame.internal._

import scala.xml.NodeSeq

trait ExtractTagsTransform extends BaseDicom {

  /**
   * Extracts value for each tag from column holding xml string and adds column for each tag to assign value. For missing tag, the value is null
   *
   * @param tags tags to extract from column holding xml string
   */
  def extractTags(tags: Seq[String]) = {
    execute(ExtractTags(tags))
  }
}

case class ExtractTags(tags: Seq[String]) extends DicomTransform {

  //The addColumns changes the state, so the changed state is returned here.
  override def work(state: DicomState): DicomState = {
    ExtractTags.extractTagsImpl(state.metadata, tags)
    state
  }
}

object ExtractTags extends Serializable {

  private implicit def rowWrapperToRowWrapperFunctions(rowWrapper: RowWrapper): RowWrapperFunctions = {
    new RowWrapperFunctions(rowWrapper)
  }

  //Get value if tag exists else return null
  def getTagValue(nodeSeqOfDicomAttribute: NodeSeq)(tag: String): String = {
    val resultNodeSeq = nodeSeqOfDicomAttribute.filter {
      da => (da \ "@tag").text == tag
    }
    if (resultNodeSeq.nonEmpty)
      resultNodeSeq.head.text
    else
      null
  }

  /**
   * Custom RowWrapper to apply on each row
   *
   * @param tags tags to add as columns
   * @return Row
   */
  private def customDicomAttributeRowWrapper(tags: Seq[String]) = {
    val rowMapper: RowWrapper => Row = row => {

      val nodeName = "DicomAttribute" //This should be node name in xml string

      //Creates NodeSeq of DicomAttribute
      val nodeSeqOfDicomAttribute = row.valueAsXmlNodeSeq(Dicom.columnName, nodeName)

      //Filter each DicomAttribute node with given tag and extract value
      val nodeValues = tags.map(getTagValue(nodeSeqOfDicomAttribute))

      //Creates a Row from given Sequence of node values
      Row.fromSeq(nodeValues)
    }
    rowMapper
  }

  /**
   * Extracts value for each tag from column holding xml string and adds column for each tag to assign value. For missing tag, the value is null
   *
   * @param metadataFrame metadata frame with column holding xml string
   * @param tags tags to extract from column holding xml string
   */
  def extractTagsImpl(metadataFrame: Frame, tags: Seq[String]) = {
    val newColumns = for (tag <- tags) yield Column(tag, DataTypes.string)
    metadataFrame.addColumns(customDicomAttributeRowWrapper(tags), newColumns)
  }

}
