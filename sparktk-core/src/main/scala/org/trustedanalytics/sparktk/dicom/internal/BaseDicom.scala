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
package org.trustedanalytics.sparktk.dicom.internal

import org.slf4j.LoggerFactory
import org.trustedanalytics.sparktk.frame.Frame

/**
 * State-backend for Dicom
 *
 * DicomState class holds metadataFrame and pixeldataFrame
 *
 * @param metadata contains id and dicom metadata as xml string
 * @param pixeldata contains id and dicom pixel data as DenseMatrix
 */
case class DicomState(val metadata: Frame, val pixeldata: Frame)

/**
 * Base Trait
 */
trait BaseDicom {

  private var dicomState: DicomState = null

  def metadata: Frame = if (dicomState != null) dicomState.metadata else null
  def pixeldata: Frame = if (dicomState != null) dicomState.pixeldata else null

  lazy val logger = LoggerFactory.getLogger("sparktk")

  private[sparktk] def init(metadata: Frame, pixeldata: Frame): Unit = {
    dicomState = DicomState(metadata, pixeldata)
  }

  protected def execute(transform: DicomTransform): Unit = {
    logger.info("Dicom transform {}", transform.getClass.getName)
    dicomState = transform.work(dicomState)
  }

  protected def execute[T](summarization: DicomSummarization[T]): T = {
    logger.info("Dicom frame summarization {}", summarization.getClass.getName)
    summarization.work(dicomState)
  }
}

trait DicomOperation extends Product {
  //def name: String
}

trait DicomSummarization[T] extends DicomOperation {
  def work(state: DicomState): T
}

trait DicomTransform extends DicomOperation {
  def work(state: DicomState): DicomState
}
