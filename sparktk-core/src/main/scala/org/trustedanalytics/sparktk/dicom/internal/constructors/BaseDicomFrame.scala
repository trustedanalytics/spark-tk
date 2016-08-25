package org.trustedanalytics.sparktk.dicom.internal.constructors
import org.slf4j.LoggerFactory
import org.trustedanalytics.sparktk.dicom.DicomFrame

/**
 * State-backend for Dicom Frame
 *
 * @param dicomFrame spark dicomFrame
 */
case class DicomFrameState(dicomFrame: DicomFrame)

/**
 * Base Trait
 */
trait BaseDicomFrame {

  var dicomFrameState: DicomFrameState = null

  def dicomFrame: DicomFrame = if (dicomFrameState != null) dicomFrameState.dicomFrame else null

  lazy val logger = LoggerFactory.getLogger("sparktk")

  protected def execute[T](summarization: DicomFrameSummarization[T]): T = {
    logger.info("Dicom frame summarization {}", summarization.getClass.getName)
    summarization.work(dicomFrameState)
  }
}

trait DicomFrameOperation extends Product {
  //def name: String
}

trait DicomFrameSummarization[T] extends DicomFrameOperation {
  def work(state: DicomFrameState): T
}

