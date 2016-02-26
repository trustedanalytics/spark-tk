package org.trustedanalytics.at.interfaces

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.trustedanalytics.at.schema.Schema

trait BaseFrame {

  private var frameState: ImmutableFrame = null

  def rdd: RDD[Row] = if (frameState != null) frameState.rdd else null
  def schema: Schema = if (frameState != null) frameState.schema else null

  protected def init(rdd: RDD[Row], schema: Schema): Unit = {
    //protected def init(rdd: RDD[Row], schema: Schema): Unit = {
    frameState = ImmutableFrame(rdd, schema)
  }

  protected def execute(transform: FrameTransform): RDD[Row] = {
    frameState = transform.work(frameState)
    println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
    if (frameState.rdd != null) {
      val c = frameState.rdd.count()
      println(s"frameState! count=$c")
      println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
    }
    frameState.rdd
  }

  protected def execute[T](summarization: FrameSummarization[T]): T = {
    summarization.work(frameState)
  }

  def say(): String = "Hi from scala"
}

trait FrameOperation extends Product {
  //def name: String
}

trait FrameTransform extends FrameOperation {
  def work(frame: ImmutableFrame): ImmutableFrame
}

trait FrameSummarization[T] extends FrameOperation {
  def work(frame: ImmutableFrame): T
}

trait FrameCreation extends FrameOperation {
  def work(): ImmutableFrame
}