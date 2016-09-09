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

