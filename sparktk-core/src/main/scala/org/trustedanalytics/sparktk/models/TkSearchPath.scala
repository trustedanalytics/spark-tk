package org.trustedanalytics.sparktk.models

import java.io.File

import scala.collection.mutable

/**
 * The SearchPath is used to find Modules and Jars
 *
 * Modules are expected jar's containing an atk-module.conf file.
 *
 * @param path list of directories delimited by colons
 */
class TkSearchPath(path: String) {

  lazy val searchPath: List[File] = path.split(":").toList.map(file => new File(file))
  println("searchPath: " + searchPath.mkString(":"))

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
    println(s"searchPath found ${files.size} jars (${results.size} of them unique) in ${System.currentTimeMillis() - startTime} milliseconds")
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
