package org.trustedanalytics.sparktk.saveload

import java.io.File
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import java.net.URI
import org.apache.hadoop.fs.permission.{ FsPermission, FsAction }
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.json4s.JsonAST.JValue
import org.json4s.jackson.Serialization
import org.json4s.{ NoTypeHints, Extraction, DefaultFormats }
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._

/**
 * Simple save/load library which uses json4s to read/write text files, including info for format validation
 */
object SaveLoad {

  /**
   * Saves a data instance to the given path as a text file
   * @param sc active spark context
   * @param path the destination path
   * @param formatId the identifier of the format type, usually the full name of a case class type
   * @param formatVersion the version of the format for the information being saved that should be recorded.
   * @param data the data to save, must be serializable to JSON using json4s
   */
  def save(sc: SparkContext, path: String, formatId: String, formatVersion: Int, data: Any) = {
    implicit val format = DefaultFormats
    val contents = compact(render(
      ("id" -> formatId) ~ ("version" -> formatVersion) ~ ("data" -> Extraction.decompose(data))))
    val sqlContext = SQLContext.getOrCreate(sc)
    sc.parallelize(Seq(contents), 1).saveAsTextFile(path)
  }

  /**
   * Stores the given zipfile (MAR) to either the HDFS or local file system
   * @param storagePath location to where the MAR file needs to be stored
   * @param zipFile the MAR file to be stored
   * @return full path to the location of the MAR file
   */
  def saveMar(storagePath: String, zipFile: File): String = {
    if (storagePath.startsWith("hdfs")) {
      val hdfsPath = new Path(storagePath)
      val hdfsFileSystem: org.apache.hadoop.fs.FileSystem = org.apache.hadoop.fs.FileSystem.get(new URI(storagePath), new Configuration())
      val localPath = new Path(zipFile.getAbsolutePath)
      hdfsFileSystem.copyFromLocalFile(false, true, localPath, hdfsPath)
      hdfsFileSystem.setPermission(hdfsPath, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.NONE))
      storagePath
    }
    else {
      val file = new File(storagePath)
      FileUtils.copyFile(zipFile, file)
      file.getCanonicalPath
    }

  }

  /**
   * Loads data from a file into a json4s JValue and provides format identifier and version
   * @param sc active spark context
   * @param path the source path
   * @return the payload from the load
   */
  def load(sc: SparkContext, path: String): LoadResult = {
    implicit val formats = DefaultFormats
    val contents = parse(sc.textFile(path).first())
    val formatId = (contents \ "id").extract[String]
    val formatVersion = (contents \ "version").extract[Int]
    val dataJValue: JValue = contents \ "data"
    LoadResult(formatId, formatVersion, dataJValue)
  }

  /**
   * Helper method to extract/create the scala object from out of the JValue
   * @param sourceJValue AST JValue representing T
   * @param t implicit reflection jazz
   * @tparam T the type to extract
   * @return new instance of T
   */
  def extractFromJValue[T <: Product](sourceJValue: JValue)(implicit t: Manifest[T]): T = {
    implicit val formats = Serialization.formats(NoTypeHints)
    sourceJValue.extract[T]
  }
}

/**
 * The results of a load operation
 *
 * @param formatId the identifier of the format type found, usually the full name of a case class type
 * @param formatVersion the version of the format for the information being loaded
 * @param data the retrieved data
 */
case class LoadResult(formatId: String, formatVersion: Int, data: JValue)