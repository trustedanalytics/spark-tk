package org.trustedanalytics.sparktk.saveload

import org.apache.spark.SparkContext
import org.json4s.JsonAST.JValue
import org.trustedanalytics.sparktk.TkContext

/**
 * Trait for companion objects of classes that want to work with the SaveLoad methodology
 */
trait TkSaveableObject {

  /**
   * Load method where the work of getting the formatVersion and tkMetadata has already been done
   *
   * @param sc active spark context
   * @param path the source path
   * @param formatVersion the version of the format for the tk metadata that should be recorded.
   * @param tkMetadata the data to save (should be a case class), must be serializable to JSON using json4s
   * @return loaded object
   */
  def loadTkSaveableObject(sc: SparkContext, path: String, formatVersion: Int, tkMetadata: JValue): Any

  /**
   * Load method for general path expecting a specific type
   * @param tc active TkContext
   * @param path the source path
   * @tparam T the type of the object expected to load
   * @return loaded object
   */
  def load[T](tc: TkContext, path: String): T = {
    tc.load(path).asInstanceOf[T]
  }

  /**
   * ID for the format of how the object is save/load-ed.  By default it is the object's type name
   * @return
   */
  def formatId: String = this.getClass.getName

  /**
   * helper which validates a given version is in the list of candidates
   * @param version version to validate
   * @param validCandidates valid versions
   */
  def validateFormatVersion(version: Int, validCandidates: Int*) = {
    require(validCandidates.contains(version),
      s"Mismatched format version during load for $formatId.  Expected $validCandidates 1, got $version")
  }
}

