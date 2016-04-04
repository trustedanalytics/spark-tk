package org.trustedanalytics.at.frame.internal

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.trustedanalytics.at.frame.Schema

trait BaseFrame {

  private var frameState: FrameState = null

  def rdd: RDD[Row] = if (frameState != null) frameState.rdd else null
  def schema: Schema = if (frameState != null) frameState.schema else null

  protected def init(rdd: RDD[Row], schema: Schema): Unit = {
    frameState = FrameState(rdd, schema)
  }

  protected def execute(transform: FrameTransform): Unit = {
    frameState = transform.work(frameState)
  }

  protected def execute[T](summarization: FrameSummarization[T]): T = {
    summarization.work(frameState)
  }

  protected def execute[T](transform: FrameTransformWithResult[T]): T = {
    val r = transform.work(frameState)
    frameState = r.state
    r.result
  }
}

trait FrameOperation extends Product {
  //def name: String
}

trait FrameTransform extends FrameOperation {
  def work(state: FrameState): FrameState
}

case class FrameTransformReturn[T](state: FrameState, result: T)

trait FrameTransformWithResult[T] extends FrameOperation {
  def work(state: FrameState): FrameTransformReturn[T]
}

trait FrameSummarization[T] extends FrameOperation {
  def work(state: FrameState): T
}

trait FrameCreation extends FrameOperation {
  def work(): FrameState
}