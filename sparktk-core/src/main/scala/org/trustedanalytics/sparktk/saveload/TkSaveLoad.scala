package org.trustedanalytics.sparktk.saveload

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext

/**
 * Library to save/load json metadata in a tk/ subfolder
 */
object TkSaveLoad {
  /**
   * Helper function for TkSaveable, this function loads TkMetadata from a file with
   * the appropriate path.  spark-tk just adds a tk/ folder alongside the data/ and metadata/ folders
   * saved by spark models.
   *
   * @param sc active spark context
   * @param path the source path
   * @return a tuple of (formatId, formatVersion, data)
   */
  def loadTk(sc: SparkContext, path: String): LoadResult = {
    SaveLoad.load(sc, tkMetadataPath(path))
  }

  /**
   * Helper function for inheritors of TkSaveable, this function saves the TkMetadata to a file with
   * the appropriate path.  spark-tk just adds a tk/ folder alongside the data/ and metadata/ folders
   * saved by spark models.
   *
   * @param sc active spark context
   * @param path the destination path
   * @param formatId the identifier of the format type, usually the full name of a case class type
   * @param formatVersion the version of the format for the tk metadata that should be recorded.
   * @param tkMetadata the data to save (should be a case class), must be serializable to JSON using json4s
   */
  def saveTk(sc: SparkContext, path: String, formatId: String, formatVersion: Int, tkMetadata: Any) = {
    SaveLoad.save(sc, tkMetadataPath(path), formatId, formatVersion, tkMetadata)
  }

  /**
   * Adds the tk/ folder onto the given path
   */
  def tkMetadataPath(path: String): String = new Path(path, "tk").toUri.toString
}
