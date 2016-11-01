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

import java.io._
import java.net.URLClassLoader
import java.nio.file.{ Path, Files }
import java.util.zip.ZipInputStream
import org.apache.commons.io.FileUtils
import org.apache.spark.api.java.JavaSparkContext
import org.slf4j.LoggerFactory
import org.trustedanalytics.scoring.interfaces.ModelReader
import org.trustedanalytics.scoring.interfaces.Model
import org.apache.spark.{ SparkConf, SparkContext }
import org.trustedanalytics.sparktk.TkContext
import org.trustedanalytics.sparktk.saveload.TkSaveableObject

/**
 * Wrapper that is able to read a MAR file containing sparktk model and related jars; and then loads and returns the Model for scoring
 */
class SparkTkModelAdapter() extends ModelReader {
  private val logger = LoggerFactory.getLogger(this.getClass)
  val MODEL_NAME = "modelClassName"

  override def read(modelZipFileInput: File, classLoader: URLClassLoader, jsonMap: Map[String, String]): Model = {
    val zipInputStream = new ZipInputStream(new FileInputStream(modelZipFileInput))
    read(zipInputStream, classLoader, jsonMap)
  }

  /**
   * Takes a zip input stream with MAR file contents for sparktk models; then loads and returns a sparktk model
   *
   * @param modelZipStreamInput stream with MAR file contents for sparktk models
   * @return loads and returns the sparktk model
   */
  override def read(modelZipStreamInput: ZipInputStream, classLoader: URLClassLoader, jsonMap: Map[String, String]): Model = {
    logger.info("Sparktk model Adapter called")
    val sparktkObject = classLoader.loadClass(jsonMap(MODEL_NAME) + "$").getField("MODULE$").get(null).asInstanceOf[TkSaveableObject]
    Thread.currentThread().setContextClassLoader(classLoader)
    val tc = createSimpleContext(modelZipStreamInput)
    sparktkObject.load(tc, getModelPath(modelZipStreamInput)).asInstanceOf[Model]
  }

  /**
   * Create a TkContext with simple, local-mode SparkContext running with local fs
   * @return a new TkContext
   */
  protected def createSimpleContext(modelZipStreamInput: ZipInputStream): TkContext = {
    val conf = new SparkConf()
      .setAppName("simple")
      .setMaster("local[1]")
      .set("spark.ui.enabled", "false")
      .set("spark.hadoop.fs.default.name", "file:///")
    val sc = new SparkContext(conf)
    new TkContext(new JavaSparkContext(sc))
  }

  /**
   * Creates and returns the path to a temporary directory holding the sparktk model
   * @param modelZipStreamInput Zip input stream containing the sparktk model
   * @return Location of the temporary directory holding the model
   */
  protected def getModelPath(modelZipStreamInput: ZipInputStream): String = {
    var tmpDir: Path = null
    val bytesIn = new Array[Byte](4096)
    var fileStr: String = null
    try {
      tmpDir = Files.createTempDirectory("sparktk-scoring")
      var entry = modelZipStreamInput.getNextEntry
      while (entry != null) {
        val individualFile = entry.getName
        //only unzip the dir containing the model
        if (!individualFile.contains(".jar") && !individualFile.contains(".json")) {
          if (individualFile.contains("/")) {
            //get the directory structure that needs to be built
            fileStr = individualFile.substring(individualFile.lastIndexOf("sparktk-scoring-model"), individualFile.lastIndexOf("/"))
            fileStr = tmpDir + fileStr.substring(fileStr.indexOf("/"), fileStr.length)
          }

          Files.createDirectories(new File(fileStr).toPath)
          val file = individualFile.toString.substring(individualFile.toString.lastIndexOf("/") + 1, individualFile.toString.length)
          val bufferedOutStream = new BufferedOutputStream(new FileOutputStream(fileStr + "/" + file))
          var read = modelZipStreamInput.read(bytesIn)
          while (read != -1) {
            bufferedOutStream.write(bytesIn, 0, read)
            read = modelZipStreamInput.read(bytesIn)
          }
          bufferedOutStream.flush()
          bufferedOutStream.close()
        }

        entry = modelZipStreamInput.getNextEntry
      }
    }
    finally {
      sys.addShutdownHook(FileUtils.deleteQuietly(tmpDir.toFile)) // Delete temporary directory on exit
    }
    tmpDir.toString
  }
}
