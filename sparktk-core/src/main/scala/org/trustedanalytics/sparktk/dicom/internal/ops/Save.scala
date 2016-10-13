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

import org.trustedanalytics.sparktk.dicom.Dicom
import org.trustedanalytics.sparktk.dicom.internal.{ DicomState, DicomSummarization, BaseDicom }
import org.trustedanalytics.sparktk.saveload.TkSaveLoad

trait SaveSummarization extends BaseDicom {
  /**
   * Save the current dicom.
   *
   * @param path The destination path.
   */
  def save(path: String): Unit = {
    execute(Save(path))
  }
}

case class Save(path: String) extends DicomSummarization[Unit] {

  override def work(state: DicomState): Unit = {
    state.metadata.dataframe.write.parquet(path + "/metadata")
    state.pixeldata.dataframe.write.parquet(path + "/pixeldata")
    val formatId = Dicom.formatId
    val formatVersion = Dicom.tkFormatVersion
    TkSaveLoad.saveTk(state.metadata.dataframe.sqlContext.sparkContext, path, formatId, formatVersion, "No Metadata")
  }
}