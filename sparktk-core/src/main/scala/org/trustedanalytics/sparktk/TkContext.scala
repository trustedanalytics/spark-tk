package org.trustedanalytics.sparktk

import org.apache.spark.api.java.JavaSparkContext
import org.trustedanalytics.sparktk.saveload.Loaders
import org.trustedanalytics.sparktk.saveload.Loaders.LoaderType

/**
 * Context for operating with sparktk
 *
 * @param jsc a live Spark Context (JavaSparkContext)
 */
class TkContext(jsc: JavaSparkContext) extends Serializable {

  val sc = jsc.sc

  /**
   * Loads a sparktk thing which has been saved at the given path
   *
   * @param path location of the sparktk thing
   * @param otherLoaders Optional loaders from other libraries, where each map entry has the format id and LoaderType.
   * @return
   */
  def load(path: String, otherLoaders: Option[Map[String, LoaderType]] = None): Any = {
    Loaders.load(sc, path, otherLoaders)
  }

  /**
   * Set the level of logging for the scala-side "sparktk" logger
   *
   * @param level "info", "warn", "error", "debug", etc.
   */
  def setLoggerLevel(level: String): Unit = {
    val logger4j = org.apache.log4j.Logger.getLogger("sparktk")
    logger4j.setLevel(org.apache.log4j.Level.toLevel(level))
  }
}
