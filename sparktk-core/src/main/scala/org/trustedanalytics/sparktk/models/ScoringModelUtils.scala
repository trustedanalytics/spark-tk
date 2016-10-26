/**
 *  Copyright (c) 2016 Intel Corporation 
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.trustedanalytics.sparktk.models

import java.io.{ FileOutputStream, File }
import java.net.URI
import java.nio.DoubleBuffer
import java.nio.file.{ Files, Path }
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.commons.io.{ IOUtils, FileUtils }
import org.trustedanalytics.model.archive.format.ModelArchiveFormat
import org.trustedanalytics.sparktk.saveload.SaveLoad

object ScoringModelUtils {
  /**
   * Attempt to cast Any type to Double
   *
   * @param value input Any type to be cast
   * @return value cast as Double, if possible
   */
  def asDouble(value: Any): Double = {
    value match {
      case null => throw new IllegalArgumentException("null cannot be converted to Double")
      case i: Int => i.toDouble
      case l: Long => l.toDouble
      case f: Float => f.toDouble
      case d: Double => d
      case bd: BigDecimal => bd.toDouble
      case s: String => s.trim().toDouble
      case _ => throw new IllegalArgumentException(s"The following value is not a numeric data type: $value")
    }
  }

  /**
   * Attempt to cast Any type to Int.  If the value cannot be converted to an integer,
   * an exception is thrown.
   * @param value input Any type to be cast
   * @return value cast as an Int, if possible
   */
  def asInt(value: Any): Int = {
    value match {
      case null => throw new IllegalArgumentException("null cannot be converted to Int")
      case i: Int => i
      case l: Long => l.toInt
      case f: Float => {
        val int = f.toInt
        if (int == f)
          int
        else
          throw new IllegalArgumentException(s"Float $value cannot be converted to an Int.")
      }
      case d: Double => {
        val int = d.toInt
        if (int == d)
          int
        else
          throw new IllegalArgumentException(s"Double $value cannot be converted to an int")
      }
      case bd: BigDecimal => {
        val int = bd.toInt
        if (int == bd)
          int
        else
          throw new IllegalArgumentException(s"BigDecimal $value cannot be converted to an int")
      }
      case s: String => s.trim().toInt
      case _ => throw new IllegalArgumentException(s"The following value is not a numeric data type: $value")
    }
  }

  /**
   * Creates a MAR zipfile for the saved model and its dependencies and stores it at the given location
   * @param marSavePath location where the MAR file should be stored
   * @param modelClass name of the model class to be loaded during scoring
   * @param modelSrcDir location where the model has been saved
   * @param modelReader Reader class for the Model, which should be used to read the .mar file.
   * @param sourcePath Path to source location.  Defaults to use the path to the currently running jar.
   * @return full path to the location of the MAR file for Scoring Engine
   */
  def saveToMar(marSavePath: String,
                modelClass: String,
                modelSrcDir: java.nio.file.Path,
                modelReader: String = classOf[SparkTkModelAdapter].getName,
                sourcePath: Option[String] = None): String = {
    val zipFile: File = File.createTempFile("modelMar", ".mar")
    val zipOutStream = new FileOutputStream(zipFile)
    try {
      if (sourcePath.isDefined)
        require(StringUtils.isNotEmpty(sourcePath.get), "sourcePath should not be null or empty")

      val absolutePath = sourcePath.getOrElse({
        val codeSource = this.getClass.getProtectionDomain.getCodeSource
        if (codeSource != null)
          codeSource.getLocation.toString
        else
          null
      })

      if (absolutePath != null) {
        println(s"saveToMar path: ${absolutePath}")
        val x = new TkSearchPath(absolutePath.substring(0, absolutePath.lastIndexOf("/")))
        var jarFileList = x.jarsInSearchPath.values.toList

        val modelFile = Files.createTempDirectory("localModel")
        val localModelPath = new org.apache.hadoop.fs.Path(modelFile.toString)
        val hdfsFileSystem: org.apache.hadoop.fs.FileSystem = org.apache.hadoop.fs.FileSystem.get(new URI(modelFile.toString), new Configuration())
        hdfsFileSystem.copyToLocalFile(new org.apache.hadoop.fs.Path(modelSrcDir.toString), localModelPath)

        jarFileList = jarFileList ::: List(new File(localModelPath.toString))
        ModelArchiveFormat.write(jarFileList, modelReader, modelClass, zipOutStream)
      }
      SaveLoad.saveMar(marSavePath, zipFile)
    }
    finally {
      FileUtils.deleteQuietly(zipFile)
      IOUtils.closeQuietly(zipOutStream)
    }
  }
}