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
package org.apache.spark.ml.org.trustedanalytics.sparktk.deeptrees.util

import java.io.IOException

import org.apache.hadoop.fs.Path
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._

import org.apache.spark.{ Logging, SparkContext }
import org.apache.spark.annotation.{ Experimental, Since }
import org.apache.spark.ml.param.{ ParamPair, Params }
import org.apache.spark.sql.SQLContext
import org.apache.spark.util.Utils

/**
 * Trait for SparkRandomForestRegressionModelMLWriter and SparkRandomForestRegressionModelMLReader.
 */
private[util] sealed trait BaseReadWrite {
  private var optionSQLContext: Option[SQLContext] = None

  /**
   * Sets the SQL context to use for saving/loading.
   */
  @Since("1.6.0")
  def context(sqlContext: SQLContext): this.type = {
    optionSQLContext = Option(sqlContext)
    this
  }

  /**
   * Returns the user-specified SQL context or the default.
   */
  protected final def sqlContext: SQLContext = {
    if (optionSQLContext.isEmpty) {
      optionSQLContext = Some(SQLContext.getOrCreate(SparkContext.getOrCreate()))
    }
    optionSQLContext.get
  }

  /** Returns the SparkRandomForestRegressionModelSparkContext underlying SparkRandomForestRegressionModelsqlContext */
  protected final def sc: SparkContext = sqlContext.sparkContext
}

/**
 * Abstract class for utility classes that can save ML instances.
 */
@Experimental
@Since("1.6.0")
abstract class MLWriter extends BaseReadWrite with Logging {

  protected var shouldOverwrite: Boolean = false

  /**
   * Saves the ML instances to the input path.
   */
  @Since("1.6.0")
  @throws[IOException]("If the input path already exists but overwrite is not enabled.")
  def save(path: String): Unit = {
    val hadoopConf = sc.hadoopConfiguration
    val outputPath = new Path(path)
    val fs = outputPath.getFileSystem(hadoopConf)
    val qualifiedOutputPath = outputPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
    if (fs.exists(qualifiedOutputPath)) {
      if (shouldOverwrite) {
        logInfo(s"Path $path already exists. It will be overwritten.")
        // TODO: Revert back to the original content if save is not successful.
        fs.delete(qualifiedOutputPath, true)
      }
      else {
        throw new IOException(
          s"Path $path already exists. Please use write.overwrite().save(path) to overwrite it.")
      }
    }
    saveImpl(path)
  }

  /**
   * SparkRandomForestRegressionModelsave() handles overwriting and then calls this method.  Subclasses should override this
   * method to implement the actual saving of the instance.
   */
  @Since("1.6.0")
  protected def saveImpl(path: String): Unit

  /**
   * Overwrites if the output path already exists.
   */
  @Since("1.6.0")
  def overwrite(): this.type = {
    shouldOverwrite = true
    this
  }

  // override for Java compatibility
  override def context(sqlContext: SQLContext): this.type = super.context(sqlContext)
}

/**
 * Trait for classes that provide SparkRandomForestRegressionModelMLWriter.
 */
@Since("1.6.0")
trait MLWritable {

  /**
   * Returns an SparkRandomForestRegressionModelMLWriter instance for this ML instance.
   */
  @Since("1.6.0")
  def write: MLWriter

  /**
   * Saves this ML instance to the input path, a shortcut of `write.save(path)`.
   */
  @Since("1.6.0")
  @throws[IOException]("If the input path already exists but overwrite is not enabled.")
  def save(path: String): Unit = write.save(path)
}

private[ml] trait DefaultParamsWritable extends MLWritable { self: Params =>

  override def write: MLWriter = new DefaultParamsWriter(this)
}

/**
 * Abstract class for utility classes that can load ML instances.
 * @tparam T ML instance type
 */
@Experimental
@Since("1.6.0")
abstract class MLReader[T] extends BaseReadWrite {

  /**
   * Loads the ML component from the input path.
   */
  @Since("1.6.0")
  def load(path: String): T

  // override for Java compatibility
  override def context(sqlContext: SQLContext): this.type = super.context(sqlContext)
}

/**
 * Trait for objects that provide SparkRandomForestRegressionModelMLReader.
 * @tparam T ML instance type
 */
@Experimental
@Since("1.6.0")
trait MLReadable[T] {

  /**
   * Returns an SparkRandomForestRegressionModelMLReader instance for this class.
   */
  @Since("1.6.0")
  def read: MLReader[T]

  /**
   * Reads an ML instance from the input path, a shortcut of `read.load(path)`.
   *
   * Note: Implementing classes should override this to be Java-friendly.
   */
  @Since("1.6.0")
  def load(path: String): T = read.load(path)
}

private[ml] trait DefaultParamsReadable[T] extends MLReadable[T] {

  override def read: MLReader[T] = new DefaultParamsReader
}

/**
 * Default SparkRandomForestRegressionModelMLWriter implementation for transformers and estimators that contain basic
 * (json4s-serializable) params and no data. This will not handle more complex params or types with
 * data (e.g., models with coefficients).
 * @param instance object to save
 */
private[ml] class DefaultParamsWriter(instance: Params) extends MLWriter {

  override protected def saveImpl(path: String): Unit = {
    DefaultParamsWriter.saveMetadata(instance, path, sc)
  }
}

private[ml] object DefaultParamsWriter {

  /**
   * Saves metadata + Params to: path + "/metadata"
   *  - class
   *  - timestamp
   *  - sparkVersion
   *  - uid
   *  - paramMap
   *  - (optionally, extra metadata)
   * @param extraMetadata  Extra metadata to be saved at same level as uid, paramMap, etc.
   * @param paramMap  If given, this is saved in the "paramMap" field.
   *                  Otherwise, all SparkRandomForestRegressionModelorg.apache.spark.ml.param.Params are encoded using
   *                  SparkRandomForestRegressionModelorg.apache.spark.ml.param.Param.jsonEncode().
   */
  def saveMetadata(
    instance: Params,
    path: String,
    sc: SparkContext,
    extraMetadata: Option[JObject] = None,
    paramMap: Option[JValue] = None): Unit = {
    val uid = instance.uid
    val cls = instance.getClass.getName
    val params = instance.extractParamMap().toSeq.asInstanceOf[Seq[ParamPair[Any]]]
    val jsonParams = paramMap.getOrElse(render(params.map {
      case ParamPair(p, v) =>
        p.name -> parse(p.jsonEncode(v))
    }.toList))
    val basicMetadata = ("class" -> cls) ~
      ("timestamp" -> System.currentTimeMillis()) ~
      ("sparkVersion" -> sc.version) ~
      ("uid" -> uid) ~
      ("paramMap" -> jsonParams)
    val metadata = extraMetadata match {
      case Some(jObject) =>
        basicMetadata ~ jObject
      case None =>
        basicMetadata
    }
    val metadataPath = new Path(path, "metadata").toString
    val metadataJson = compact(render(metadata))
    sc.parallelize(Seq(metadataJson), 1).saveAsTextFile(metadataPath)
  }

  /**
   * Helper for SparkRandomForestRegressionModelsaveMetadata() which extracts the JSON to save.
   * This is useful for ensemble models which need to save metadata for many sub-models.
   *
   * @see SparkRandomForestRegressionModelsaveMetadata() for details on what this includes.
   */
  def getMetadataToSave(
    instance: Params,
    sc: SparkContext,
    extraMetadata: Option[JObject] = None,
    paramMap: Option[JValue] = None): String = {
    val uid = instance.uid
    val cls = instance.getClass.getName
    val params = instance.extractParamMap().toSeq.asInstanceOf[Seq[ParamPair[Any]]]
    val jsonParams = paramMap.getOrElse(render(params.map {
      case ParamPair(p, v) =>
        p.name -> parse(p.jsonEncode(v))
    }.toList))
    val basicMetadata = ("class" -> cls) ~
      ("timestamp" -> System.currentTimeMillis()) ~
      ("sparkVersion" -> sc.version) ~
      ("uid" -> uid) ~
      ("paramMap" -> jsonParams)
    val metadata = extraMetadata match {
      case Some(jObject) =>
        basicMetadata ~ jObject
      case None =>
        basicMetadata
    }
    val metadataJson: String = compact(render(metadata))
    metadataJson
  }
}

/**
 * Default SparkRandomForestRegressionModelMLReader implementation for transformers and estimators that contain basic
 * (json4s-serializable) params and no data. This will not handle more complex params or types with
 * data (e.g., models with coefficients).
 * @tparam T ML instance type
 * TODO: Consider adding check for correct class name.
 */
private[ml] class DefaultParamsReader[T] extends MLReader[T] {

  override def load(path: String): T = {
    val metadata = DefaultParamsReader.loadMetadata(path, sc)
    val cls = Utils.classForName(metadata.className)
    val instance =
      cls.getConstructor(classOf[String]).newInstance(metadata.uid).asInstanceOf[Params]
    DefaultParamsReader.getAndSetParams(instance, metadata)
    instance.asInstanceOf[T]
  }
}

private[ml] object DefaultParamsReader {

  /**
   * All info from metadata file.
   * @param params  paramMap, as a SparkRandomForestRegressionModelJValue
   * @param metadata  All metadata, including the other fields
   * @param metadataJson  Full metadata file String (for debugging)
   */
  case class Metadata(
      className: String,
      uid: String,
      timestamp: Long,
      sparkVersion: String,
      params: JValue,
      metadata: JValue,
      metadataJson: String) {

    /**
     * Get the JSON value of the SparkRandomForestRegressionModelorg.apache.spark.ml.param.Param of the given name.
     * This can be useful for getting a Param value before an instance of `Params`
     * is available.
     */
    def getParamValue(paramName: String): JValue = {
      implicit val format = DefaultFormats
      params match {
        case JObject(pairs) =>
          val values = pairs.filter {
            case (pName, jsonValue) =>
              pName == paramName
          }.map(_._2)
          assert(values.length == 1, s"Expected one instance of Param '$paramName' but found" +
            s" ${values.length} in JSON Params: " + pairs.map(_.toString).mkString(", "))
          values.head
        case _ =>
          throw new IllegalArgumentException(
            s"Cannot recognize JSON metadata: $metadataJson.")
      }
    }
  }

  /**
   * Load metadata from file.
   * @param expectedClassName  If non empty, this is checked against the loaded metadata.
   * @throws IllegalArgumentException if expectedClassName is specified and does not match metadata
   */
  def loadMetadata(path: String, sc: SparkContext, expectedClassName: String = ""): Metadata = {
    val metadataPath = new Path(path, "metadata").toString
    val metadataStr = sc.textFile(metadataPath, 1).first()
    val metadata = parse(metadataStr)

    implicit val format = DefaultFormats
    val className = (metadata \ "class").extract[String]
    val uid = (metadata \ "uid").extract[String]
    val timestamp = (metadata \ "timestamp").extract[Long]
    val sparkVersion = (metadata \ "sparkVersion").extract[String]
    val params = metadata \ "paramMap"
    if (expectedClassName.nonEmpty) {
      require(className == expectedClassName, s"Error loading metadata: Expected class name" +
        s" $expectedClassName but found class name $className")
    }

    Metadata(className, uid, timestamp, sparkVersion, params, metadata, metadataStr)
  }

  /**
   * Parse metadata JSON string produced by SparkRandomForestRegressionModelDefaultParamsWriter.getMetadataToSave().
   * This is a helper function for SparkRandomForestRegressionModelloadMetadata().
   *
   * @param metadataStr  JSON string of metadata
   * @param expectedClassName  If non empty, this is checked against the loaded metadata.
   * @throws IllegalArgumentException if expectedClassName is specified and does not match metadata
   */
  def parseMetadata(metadataStr: String, expectedClassName: String = ""): Metadata = {
    val metadata = parse(metadataStr)

    implicit val format = DefaultFormats
    val className = (metadata \ "class").extract[String]
    val uid = (metadata \ "uid").extract[String]
    val timestamp = (metadata \ "timestamp").extract[Long]
    val sparkVersion = (metadata \ "sparkVersion").extract[String]
    val params = metadata \ "paramMap"
    if (expectedClassName.nonEmpty) {
      require(className == expectedClassName, s"Error loading metadata: Expected class name" +
        s" $expectedClassName but found class name $className")
    }

    Metadata(className, uid, timestamp, sparkVersion, params, metadata, metadataStr)
  }

  /**
   * Extract Params from metadata, and set them in the instance.
   * This works if all Params implement SparkRandomForestRegressionModelorg.apache.spark.ml.param.Param.jsonDecode().
   */
  def getAndSetParams(instance: Params, metadata: Metadata): Unit = {
    implicit val format = DefaultFormats
    metadata.params match {
      case JObject(pairs) =>
        pairs.foreach {
          case (paramName, jsonValue) =>
            val param = instance.getParam(paramName)
            val value = param.jsonDecode(compact(render(jsonValue)))
            instance.set(param, value)
        }
      case _ =>
        throw new IllegalArgumentException(
          s"Cannot recognize JSON metadata: ${metadata.metadataJson}.")
    }
  }

  /**
   * Load a SparkRandomForestRegressionModelParams instance from the given path, and return it.
   * This assumes the instance implements SparkRandomForestRegressionModelMLReadable.
   */
  def loadParamsInstance[T](path: String, sc: SparkContext): T = {
    val metadata = DefaultParamsReader.loadMetadata(path, sc)
    val cls = Utils.classForName(metadata.className)
    cls.getMethod("read").invoke(null).asInstanceOf[MLReader[T]].load(path)
  }
}
