package org.trustedanalytics.sparktk

import org.apache.spark.api.java.JavaSparkContext
import org.trustedanalytics.sparktk.frame.Frame
import org.trustedanalytics.sparktk.frame.internal.ops.Load
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd

class TkContext(jsc: JavaSparkContext) extends Serializable {

  private val sc = jsc.sc

  def helloWorld(): String = "Hello from TK"

  def loadFrame(path: String): Frame = {
    val frameRdd: FrameRdd = Load.loadParquet(path, sc)
    new Frame(frameRdd, frameRdd.frameSchema)
  }
}
