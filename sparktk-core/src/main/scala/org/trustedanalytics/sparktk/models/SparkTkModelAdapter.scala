/**
 *  Copyright (c) 2015 Intel Corporation 
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
import java.util.Scanner
import java.util.zip.ZipInputStream
import org.apache.commons.io.FileUtils
import org.apache.spark.api.java.JavaSparkContext
import org.slf4j.LoggerFactory
import org.trustedanalytics.scoring.interfaces.ModelReader
import org.trustedanalytics.scoring.interfaces.Model
import org.apache.spark.{ SparkConf, SparkContext }
import org.trustedanalytics.sparktk.TkContext
import org.trustedanalytics.sparktk.saveload.TkSaveableObject
import java.util.Scanner

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
    println("***************************************************")
    val ur = classLoader.getURLs
    ur.foreach { u => println(u) }
    println("***************************************************")
    val sparktkObject = classLoader.loadClass(jsonMap(MODEL_NAME) + "$").getField("MODULE$").get(null).asInstanceOf[TkSaveableObject]

    val tc = createSimpleContext()
    println("tc obtained")
    //val sparktkModel = tc.load(getModelPath(modelZipStreamInput))
    sparktkObject.load(tc, getModelPath(modelZipStreamInput)).asInstanceOf[Model]
  }

  /**
   * Create a TkContext with simple, local-mode SparkContext running with local fs
   * @return a new TkContext
   */
  private def createSimpleContext(): TkContext = {
    import org.apache.spark.rpc.netty
    val conf = new SparkConf()
      .setAppName("simple")
      .setMaster("local[1]")
      .set("spark.hadoop.fs.default.name", "file:///")
    val sc = new SparkContext(conf)
    new TkContext(new JavaSparkContext(sc))
  }

  /**
   *
   * @param modelZipStreamInput
   * @return
   */
  private def getModelPath(modelZipStreamInput: ZipInputStream): String = {
    val in = new Scanner(System.in)
    var tmpDir: Path = null
    val readBuffer = new Array[Byte](4096)
    try {
      tmpDir = Files.createTempDirectory("sparktk-scoring")
      var entry = modelZipStreamInput.getNextEntry
      while (entry != null) {
        val individualFile = entry.getName
        //only unzip the dir containing the model
        if (!individualFile.contains(".jar") && !individualFile.contains(".json")) {

          if (individualFile.contains("topicsGivenDocFrame")) {
            fileStr = tmpDir + "/topicsGivenDocFrame"
          }
          else if (individualFile.contains("distLdaModel/data/globalTopicTotals")) {
            fileStr = tmpDir + "/distLdaModel/data/globalTopicTotals"
          }
          else if (individualFile.contains("distLdaModel/data/tokenCounts")) {
            fileStr = tmpDir + "/distLdaModel/data/tokenCounts"
          }
          else if (individualFile.contains("distLdaModel/data/topicCounts")) {
            fileStr = tmpDir + "/distLdaModel/data/topicCounts"
          }
          else if (individualFile.contains("distLdaModel/metadata")) {
            fileStr = tmpDir + "/distLdaModel/metadata"
          }
          else if (individualFile.contains("topicsGivenWordFrame")) {
            fileStr = tmpDir + "/topicsGivenWordFrame"
          }
          else if (individualFile.contains("wordGivenTopicsFrame")) {
            fileStr = tmpDir + "/wordGivenTopicsFrame"
          }
          else if (individualFile.contains("wordGivenTopicsFrame")) {
            fileStr = tmpDir + "/wordGivenTopicsFrame"
          }
          else if (individualFile.contains("wordGivenTopicsFrame")) {
            fileStr = tmpDir + "/wordGivenTopicsFrame"
          }
          else if (individualFile.contains("metadata")) {
            fileStr = tmpDir + "/metadata"
          }
          else {
            val parent = file.getParentFile
            if (parent != null) {
              parent.mkdirs()
              println("making parent dir")
            }
            val fileoutStream = new FileOutputStream(file)
            var read = modelZipStreamInput.read(readBuffer)
            while (read != -1) {
              fileoutStream.write(readBuffer, 0, read)
              read = modelZipStreamInput.read(readBuffer)
            }
            fileoutStream.close
          }
        }

        entry = modelZipStreamInput.getNextEntry
      }

    }
    finally {
      sys.addShutdownHook(FileUtils.deleteQuietly(tmpDir.toFile)) // Delete temporary directory on exit
    }
    val i = in.nextInt()
    tmpDir.toString
  }

  private def extractFile(zipIn: ZipInputStream, tempDir: String, filePath: String): Unit = {
    var file: File = null
    var bufferedOutStream: BufferedOutputStream = null
    val bytesIn = new Array[Byte](4096)

    try {
      file = new File(tempDir, filePath)
      file.createNewFile()

      bufferedOutStream = new BufferedOutputStream(new FileOutputStream(file))
      var read = zipIn.read(bytesIn)
      while (read != -1) {
        bufferedOutStream.write(bytesIn, 0, read)
        read = zipIn.read(bytesIn)
      }
    }
    finally {
      bufferedOutStream.close()
    }
  }
}
