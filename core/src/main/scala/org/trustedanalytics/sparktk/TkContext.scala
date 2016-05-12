package org.trustedanalytics.sparktk

import org.apache.spark.api.java.JavaSparkContext
import org.trustedanalytics.sparktk.saveload.Loaders

class TkContext(jsc: JavaSparkContext) extends Serializable {

  val sc = jsc.sc

  def load(path: String): Any = {
    Loaders.load(sc, path)
  }
}
