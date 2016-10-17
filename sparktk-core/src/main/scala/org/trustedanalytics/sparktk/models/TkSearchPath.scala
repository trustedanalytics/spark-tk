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

import java.io.File
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
 * The SearchPath is used to find Modules and Jars
 *
 * Modules are expected jar's containing an atk-module.conf file.
 *
 * @param path list of directories delimited by colons
 */
class TkSearchPath(path: String) {
  private val logger = LoggerFactory.getLogger(this.getClass)
  lazy val searchPath: List[File] = path.split(":").toList.map(file => new File(file))
  logger.info("searchPath: " + searchPath.mkString(":"))

  lazy val jarsInSearchPath: Map[String, File] = {
    val startTime = System.currentTimeMillis()
    val files = searchPath.flatMap(recursiveListOfJars)
    val results = mutable.Map[String, File]()
    for (file <- files) {
      // only take the first jar with a given name on the search path
      if (!results.contains(file.getName)) {
        results += (file.getName -> file)
      }
    }
    // debug to make sure we're not taking forever when someone adds some huge Maven repo to search path
    logger.info(s"searchPath found ${files.size} jars (${results.size} of them unique) in ${System.currentTimeMillis() - startTime} milliseconds")
    results.toMap
  }

  /**
   * Recursively find jars under a directory
   */
  def recursiveListOfJars(dir: File): Array[File] = {
    if (dir.exists()) {
      require(dir.isDirectory, s"Only directories are allowed in the search path: '${dir.getAbsolutePath}' was not a directory")
      val files = dir.listFiles()
      val jars = files.filter(f => f.exists() && f.getName.endsWith(".jar"))
      jars ++ files.filter(_.isDirectory).flatMap(recursiveListOfJars)
    }
    else {
      Array.empty
    }
  }
}
