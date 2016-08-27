package org.trustedanalytics.sparktk.dicom.internal

import org.slf4j.LoggerFactory
import org.trustedanalytics.sparktk.dicom.DicomFrame

/**
 * State-backend for Dicom Frame
 *
 * @param dicomFrame spark dicomFrame
 */
case class DicomState(dicomFrame: DicomFrame)

/**
 * Base Trait
 */
trait BaseDicom {

  var dicomState: DicomState = null

  def dicomFrame: DicomFrame = if (dicomState != null) dicomState.dicomFrame else null

  lazy val logger = LoggerFactory.getLogger("sparktk")

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

