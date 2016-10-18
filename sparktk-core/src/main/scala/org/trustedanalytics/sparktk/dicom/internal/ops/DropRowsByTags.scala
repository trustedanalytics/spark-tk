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
