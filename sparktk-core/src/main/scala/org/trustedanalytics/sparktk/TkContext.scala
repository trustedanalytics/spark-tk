package org.trustedanalytics.sparktk

import java.util.Properties
import org.slf4j.LoggerFactory
import org.apache.maven.artifact.versioning.DefaultArtifactVersion

import org.apache.spark.api.java.JavaSparkContext
import org.trustedanalytics.sparktk.saveload.Loaders

/**
 * Context for operating with sparktk
 *
 * @param jsc a live Spark Context (JavaSparkContext)
 */
class TkContext(jsc: JavaSparkContext) extends Serializable {

  val sc = jsc.sc

  private val sparkBuildVersion = {
    val propertiesFile = this.getClass.getResourceAsStream("/maven.properties")
    val properties = new Properties()
    properties.load(propertiesFile)
    new DefaultArtifactVersion(properties.getProperty("dep.spark.version")) // From Maven
  }
  private val sparkRuntimeVersion = new DefaultArtifactVersion(sc.version) // From environment
  private val logger = LoggerFactory.getLogger(this.getClass)

  private def checkSparkBuildVersionCompatibility() = {
    if (sparkBuildVersion.getMajorVersion == sparkRuntimeVersion.getMajorVersion &&
      sparkBuildVersion.getMinorVersion == sparkRuntimeVersion.getMinorVersion) {
      if (sparkBuildVersion.getIncrementalVersion != sparkRuntimeVersion.getIncrementalVersion)
        logger.warn(s"Incremental version mismatch: Spark Build Version $sparkBuildVersion " +
          s"Spark Runtime $sparkRuntimeVersion")
      true
    }
    else false
  }

  require(checkSparkBuildVersionCompatibility(), s"Spark Build Version $sparkBuildVersion " +
    s"is not compatible with Spark Runtime $sparkRuntimeVersion")

  /**
   * Loads a sparktk thing which has been saved at the given path
   *
   * @param path location of the sparktk thing
   * @return
   */
  def load(path: String): Any = {
    Loaders.load(sc, path)
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
