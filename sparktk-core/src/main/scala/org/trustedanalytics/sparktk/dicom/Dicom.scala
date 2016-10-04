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
package org.trustedanalytics.sparktk.dicom

import org.apache.spark.SparkContext
import org.json4s.JsonAST.JValue
import org.trustedanalytics.sparktk.dicom.internal.BaseDicom
import org.trustedanalytics.sparktk.dicom.internal.ops.{ ExtractTagsTransform, ExtractKeywordsTransform, SaveSummarization }
import org.trustedanalytics.sparktk.frame.Frame
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.trustedanalytics.sparktk.saveload.TkSaveableObject

/**
 * Dicom holds metadata and pixeldata frames
 *
 * @param metadata dicom metadata frame
 * @param pixeldata dicom pixeldata frame
 */
class Dicom(metadata: Frame, pixeldata: Frame) extends BaseDicom with Serializable
    with ExtractKeywordsTransform
    with ExtractTagsTransform
    with SaveSummarization {
  super.init(metadata, pixeldata)
}

object Dicom extends TkSaveableObject {

  val metadataColumnName = "metadata" //Name of the column holding xml string as value in a frame. To access while convert Row to NodeSeq.
  val tkFormatVersion = 1

  /**
   * Loads the parquet files (the metadata and pixeldata dataframes) found at the given path and returns a DicomFrame
   *
   * @param sc active SparkContext
   * @param path path to the file
   * @param formatVersion TK metadata formatVersion
   * @param tkMetadata TK metadata
   * @return
   */
  def loadTkSaveableObject(sc: SparkContext, path: String, formatVersion: Int = tkFormatVersion, tkMetadata: JValue = null): Any = {
    require(tkFormatVersion == formatVersion, s"DicomFrame load only supports version $tkFormatVersion.  Got version $formatVersion")
    // no extra metadata in version 1
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val metadataDF = sqlContext.read.parquet(path + "/metadata")
    val imageDF = sqlContext.read.parquet(path + "/pixeldata")

    val metadataFrameRdd = FrameRdd.toFrameRdd(metadataDF)
    val metadataFrame = new Frame(metadataFrameRdd, metadataFrameRdd.frameSchema)

    val pixeldataFrameRdd = FrameRdd.toFrameRdd(imageDF)
    val pixeldataFrame = new Frame(pixeldataFrameRdd, pixeldataFrameRdd.frameSchema)

    new Dicom(metadataFrame, pixeldataFrame)
  }
}

