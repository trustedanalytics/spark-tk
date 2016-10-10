/**
 *  Copyright (c) 2016 Intel Corporation 
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.trustedanalytics.sparktk.dicom.internal.ops

import org.apache.spark.sql.Row
import org.trustedanalytics.sparktk.dicom.Dicom
import org.trustedanalytics.sparktk.dicom.internal.{ BaseDicom, DicomTransform, DicomState }
import org.trustedanalytics.sparktk.frame.internal.rdd.RowWrapperFunctions
import org.trustedanalytics.sparktk.frame._
import org.trustedanalytics.sparktk.frame.internal._

trait FilterByKeywordsTransform extends BaseDicom {

  /**
   * Filter the rows based on Map(keyword, value) from column holding xml string
   *
   * @param keywordsValuesMap Map with keyword and associated value from xml string
   */
  def filterByKeywords(keywordsValuesMap: Map[String, String]) = {
    execute(FilterByKeywords(keywordsValuesMap))
  }
}

case class FilterByKeywords(keywordsValuesMap: Map[String, String]) extends DicomTransform {

  override def work(state: DicomState): DicomState = {
    FilterByKeywords.filterOrDropByKeywordsImpl(state.metadata, keywordsValuesMap, isDropRows = false)
    val filteredIdFrame = state.metadata.copy(Some(Map("id" -> "id")))
    val filteredPixeldata = filteredIdFrame.joinInner(state.pixeldata, List("id"))
    DicomState(state.metadata, filteredPixeldata)
  }
}

object FilterByKeywords extends Serializable {

  //create RowWrapper from Row to access valueAsXmlNodeSeq method
  private implicit def rowWrapperToRowWrapperFunctions(row: Row)(implicit schema: Schema): RowWrapperFunctions = {
    val rowWrapper = new RowWrapper(schema).apply(row)
    new RowWrapperFunctions(rowWrapper)
  }

  //custom filter based on given keywordsValuesMap
  def customKeywordsFunc(keywordsValuesMap: Map[String, String])(row: Row)(implicit schema: Schema): Boolean = {

    //Creates NodeSeq of DicomAttribute
    val nodeSeqOfDicomAttribute = row.valueAsXmlNodeSeq(Dicom.metadataColumnName, Dicom.nodeNameInMetadata)

    // Loop through each DicomAttribute and check for the given keyword.
    // If keyword is found returns True else False. Apply 'and' operation on Booleans and return final Boolean value,
    // filter uses Boolean value to decide whether to keep the record or drop
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
   * Filter or Drop the rows based on Map(keyword, value) from column holding xml string
   *
   * @param metadataFrame metadata frame with column holding xml string
   * @param keywordsValuesMap  Map with keyword and associated value from xml string
   */
  def filterOrDropByKeywordsImpl(metadataFrame: Frame, keywordsValuesMap: Map[String, String], isDropRows: Boolean) = {
    implicit val schema = metadataFrame.schema
    if (isDropRows)
      metadataFrame.dropRows(customKeywordsFunc(keywordsValuesMap))
    else
      metadataFrame.filter(customKeywordsFunc(keywordsValuesMap))
  }

}