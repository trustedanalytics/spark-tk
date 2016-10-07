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
package org.trustedanalytics.sparktk.models.timeseries.arima

import java.util.zip.ZipOutputStream

import com.google.common.base.Charsets
import org.apache.commons.lang.StringUtils
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.SparkContext
import org.json4s.JsonAST.JValue
import org.trustedanalytics.sparktk.TkContext
import org.trustedanalytics.sparktk.saveload.{ SaveLoad, TkSaveLoad, TkSaveableObject }
import com.cloudera.sparkts.models.{ ARIMA => SparkTsArima, ARIMAModel => SparkTsArimaModel }
import org.trustedanalytics.scoring.interfaces.{ ModelMetaDataArgs, Field, Model }
import org.trustedanalytics.sparktk.models.ScoringModelUtils
import org.trustedanalytics.model.archive.format.ModelArchiveFormat
import java.io.{ BufferedOutputStream, FileOutputStream, File }
import org.apache.commons.io.{ FileUtils, IOUtils }
import spray.json._
import DefaultJsonProtocol._

object ArimaModel extends TkSaveableObject {

  /**
   * Current format version for model save/load
   */
  private val currentFormatVersion: Int = 1

  /**
   * List of format version that are valid for loading
   */
  private val validFormatVersions = List[Int](currentFormatVersion)

  /**
   * Creates Autoregressive Integrated Moving Average (ARIMA) Model from the specified time series values.
   *
   * Given a time series, fits an non-seasonal Autoregressive Integrated Moving Average (ARIMA) model of
   * order (p, d, q) where p represents the autoregression terms, d represents the order of differencing,
   * and q represents the moving average error terms.  If includeIntercept is true, the model is fitted
   * with an intercept.
   *
   * @param ts Time series to which to fit an ARIMA(p, d, q) model.
   * @param p Autoregressive order
   * @param d Differencing order
   * @param q Moving average order
   * @param includeIntercept If true, the model is fit with an intercept.  Default is True.
   * @param method Objective function and optimization method.  Current options are: 'css-bobyqa'
   *               and 'css-cgd'.  Both optimize the log likelihood in terms of the conditional sum of
   *               squares.  The first uses BOBYQA for optimization, while the second uses conjugate
   *               gradient descent.  Default is 'css-cgd'.
   * @param initParams A set of user provided initial parameters for optimization. If the list is empty
   *                   (default), initialized using Hannan-Rissanen algorithm. If provided, order of parameter
   *                   should be: intercept term, AR parameters (in increasing order of lag), MA parameters
   *                   (in increasing order of lag).
   * @return Trained ARIMA model
   */
  def train(ts: Seq[Double],
            p: Int,
            d: Int,
            q: Int,
            includeIntercept: Boolean = true,
            method: String = "css-cgd",
            initParams: Option[Seq[Double]] = None): ArimaModel = {
    require(ts != null && ts.nonEmpty, "ts list cannot be null nor empty.")
    require(StringUtils.isNotEmpty(method), "method should not be null or empty")
    val trainMethod = method.toLowerCase
    require(List("css-bobyqa", "css-cgd").contains(trainMethod), "method string should be 'css-cgd' or 'css-bobyqa'.")

    // Fit model using the specified time series values and ARIMA parameters
    val userInitParams = if (initParams.isDefined) initParams.get.toArray else null
    val ts_vector = new DenseVector(ts.toArray)
    val arimaModel = SparkTsArima.fitModel(p, d, q, ts_vector, includeIntercept, trainMethod, userInitParams)

    // Return trained model
    ArimaModel(ts_vector, p, d, q, includeIntercept, method, initParams, arimaModel)
  }

  /**
   * Load a ArimaModel from the given path
   * @param tc TkContext
   * @param path location
   * @return
   */
  def load(tc: TkContext, path: String): ArimaModel = {
    tc.load(path).asInstanceOf[ArimaModel]
  }

  /**
   * Load model from file
   *
   * @param sc active spark context
   * @param path the source path
   * @param formatVersion the version of the format for the tk metadata that should be recorded.
   * @param tkMetadata the data to save (should be a case class), must be serializable to JSON using json4s
   * @return ArimaModel loaded from the specified file
   */
  override def loadTkSaveableObject(sc: SparkContext, path: String, formatVersion: Int, tkMetadata: JValue): Any = {
    validateFormatVersion(formatVersion, validFormatVersions: _*)
    val m: ArimaModelTkMetaData = SaveLoad.extractFromJValue[ArimaModelTkMetaData](tkMetadata)

    // Define the spark-ts ARIMA model
    val sparkModel = new SparkTsArimaModel(m.p, m.d, m.q, m.coefficients, m.hasIntercept)

    // Create ArimaModel to return
    ArimaModel(m.ts, m.p, m.d, m.q, m.includeIntercept, m.method, m.initParams, sparkModel)
  }
}

object ARIMAModelFormat extends JsonFormat[(SparkTsArimaModel, DenseVector)] {

  /**
   * Converts from an ARIMAModel to a JsValue
   * @param obj ARIMAModel where the format is
   *            p : scala.Int
   *            d : scala.Int
   *            q : scala.Int
   *            coefficients : scala:Array[scala.Double]
   *            hasIntercept : scala.Boolean
   * @return JsValue
   */
  override def write(obj: (SparkTsArimaModel, DenseVector)): JsValue = {
    val model = obj._1
    val ts = obj._2
    JsObject(
      "p" -> model.p.toJson,
      "d" -> model.d.toJson,
      "q" -> model.q.toJson,
      "coefficients" -> model.coefficients.toJson,
      "hasIntercept" -> model.hasIntercept.toJson,
      "ts" -> ts.toArray.toJson
    )
  }

  /**
   * Reads a JsValue and returns an ARIMAModel.
   * @param json JsValue
   * @return ARIMAModel where the format is
   *            p : scala.Int
   *            d : scala.Int
   *            q : scala.Int
   *            coefficients : scala:Array[scala.Double]
   *            hasIntercept : scala.Boolean
   *         And DenseVector with the time series values
   */
  override def read(json: JsValue): (SparkTsArimaModel, DenseVector) = {
    val fields = json.asJsObject.fields
    val p = getOrInvalid(fields, "p").convertTo[Int]
    val d = getOrInvalid(fields, "d").convertTo[Int]
    val q = getOrInvalid(fields, "q").convertTo[Int]
    val coefficients = getOrInvalid(fields, "coefficients").convertTo[Array[Double]]
    val hasIntercept = getOrInvalid(fields, "hasIntercept").convertTo[Boolean]
    val ts = getOrInvalid(fields, "ts").convertTo[Array[Double]]
    val model = new SparkTsArimaModel(p, d, q, coefficients, hasIntercept)
    (model, new DenseVector(ts))
  }

  def getOrInvalid[T](map: Map[String, T], key: String): T = {
    // throw exception if a programmer made a mistake
    map.getOrElse(key, deserializationError(s"expected key $key was not found in JSON $map"))
  }
}

case class ArimaModel private[arima] (ts: DenseVector,
                                      p: Int,
                                      d: Int,
                                      q: Int,
                                      includeIntercept: Boolean,
                                      method: String,
                                      initParams: Option[Seq[Double]],
                                      arimaModel: SparkTsArimaModel) extends Serializable with Model {

  /**
   * Name of scoring model reader
   */
  private val modelReader: String = "SparkTkModelReader"

  /**
   * Coefficient values: intercept, AR, MA, with increasing degrees
   */
  def coefficients: Seq[Double] = arimaModel.coefficients

  /**
   * Time series values that the model was trained with
   */
  def timeseriesValues: Seq[Double] = ts.toArray

  /**
   * Forecasts future periods using ARIMA.
   *
   * Provided fitted values of the time series as 1-step ahead forecasts, based on current model parameters, then
   * provide future periods of forecast.  We assume AR terms prior to the start of the series are equal to the
   * model's intercept term (or 0.0, if fit without an intercept term).  Meanwhile, MA terms prior to the start
   * are assumed to be 0.0.  If there is differencing, the first d terms come from the original series.
   *
   * @param futurePeriods Periods in the future to forecast (beyond length of time series that the model was
   *                      trained with)
   * @param tsValues Optional list of time series values to use as the gold standard.  If time series values are not
   *                 provided, the values used for training will be used for forecasting.
   * @return a series consisting of fitted 1-step ahead forecasts for historicals and then
   *         the number of future periods of forecasts. Note that in the future values error terms become
   *         zero and prior predictions are used for any AR terms.
   */
  def predict(futurePeriods: Int, tsValues: Option[Seq[Double]] = None): Seq[Double] = {
    require(futurePeriods >= 0, "number of futurePeriods should be a positive value.")

    val tsPredictValues = if (tsValues.isDefined) new DenseVector(tsValues.get.toArray) else ts

    // Call the ARIMA model to forecast values using the specified golden value
    arimaModel.forecast(tsPredictValues, futurePeriods).toArray
  }

  /**
   * ARIMA Model save
   * @param sc spark context
   * @param path Path to save model
   */
  def save(sc: SparkContext, path: String): Unit = {
    val tkMetadata = ArimaModelTkMetaData(ts, p, d, q, includeIntercept, method, initParams, arimaModel.hasIntercept, arimaModel.coefficients)
    TkSaveLoad.saveTk(sc, path, ArimaModel.formatId, ArimaModel.currentFormatVersion, tkMetadata)
  }

  override def score(data: Array[Any]): Array[Any] = {
    require(data != null && data.length > 0, "scoring data must not be null nor empty")

    // Scoring expects the data array to contain:
    //  (1) an integer for the number of future values to forecast
    //  (2) optional list of time series values
    if (data.length != 1 && data.length != 2)
      throw new IllegalArgumentException(s"Unexpected number of elements in the data array.  The ARIMA score model expects 1 or 2 elements, but received ${data.length}")

    if (data(0).isInstanceOf[Int] == false)
      throw new IllegalArgumentException(s"The ARIMA score model expects the first item in the data array to be an integer.  Instead received ${data(0).getClass.getSimpleName}.")

    // Get number of future periods from the first item in the data array
    val futurePeriods = ScoringModelUtils.asInt(data(0))

    // If there is a second item in the data array, it should be a list of doubles (to use as the timeseries vector)
    val timeseries: Option[Seq[Double]] = if (data.length == 2) {
      data(1) match {
        case tsList: Array[_] => Some(tsList.map(ScoringModelUtils.asDouble(_)).toArray)
        case _ => throw new IllegalArgumentException(s"The ARIMA score model expectes the second item in the data array to be a " +
          s"Array[Double].  Instead received ${data(1).getClass.getSimpleName} ")
      }
    }
    else None

    data :+ predict(futurePeriods, timeseries).toArray
  }

  override def modelMetadata(): ModelMetaDataArgs = {
    new ModelMetaDataArgs("ARIMA Model", classOf[SparkTsArimaModel].getName, modelReader, Map())
  }

  override def input(): Array[Field] = {
    Array[Field](Field("future", "Int"), Field("timeseries", "Array[Double]"))
  }

  override def output(): Array[Field] = {
    var output = input()
    output :+ Field("predicted_values", "Array[Double]")
  }

  def exportToMar(sc: SparkContext, path: String): Unit = {
    val marFile: File = new File(path)
    val directory = marFile.getParentFile
    val tempModelSavePath = directory.getAbsolutePath + "/model-save"
    save(sc, tempModelSavePath)
    val savedModelFile = new File(tempModelSavePath)
    val marFileOutputStream = new FileOutputStream(marFile)
    //val marArchive = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(marFile)))

    try {

      ModelArchiveFormat.write(List(savedModelFile), modelReader, classOf[SparkTsArimaModel].getName, marFileOutputStream)

    }
    finally {
      IOUtils.closeQuietly(marFileOutputStream)
      FileUtils.deleteQuietly(savedModelFile)
    }
    //    val dir = sys.env("SPARKTK_HOME")
    //    val f = new File(dir)
    // TODO: Implement exportToMar
    //throw new NotImplementedError("exportToMar is not implemented yet")

    //    val testZipFile = File.createTempFile("TestZip", ".mar")
    //    val testJarFile = File.createTempFile("test", ".jar")
    //    val testZipArchive = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(testZipFile)))
    //    val modelLoaderFile = File.createTempFile("descriptor", ".json")
    //
    //    try {
    //      ModelArchiveFormat.addFileToZip(testZipArchive, testJarFile)
    //      ModelArchiveFormat.addByteArrayToZip(testZipArchive, "descriptor.json", 256, "{\"modelLoaderClassName\": \"org.trustedanalytics.model.archive.format.TestModelReader\"}".getBytes("utf-8"))
    //
    //      testZipArchive.finish()
    //      IOUtils.closeQuietly(testZipArchive)
    //
    //      val testModel = ModelArchiveFormat.read(testZipFile, this.getClass.getClassLoader, None)
    //
    //      assert(testModel.isInstanceOf[Model])
    //      assert(testModel != null)
    //    }
    //    catch {
    //      case e: Exception =>
    //        throw e
    //    }
    //    finally {
    //      FileUtils.deleteQuietly(modelLoaderFile)
    //      FileUtils.deleteQuietly(testZipFile)
    //      FileUtils.deleteQuietly(testJarFile)
    //    }
    //  }
  }
}

/**
 * TK Metadata that will be stored as part of the ARIMA model
 *
 * @param ts vector of time series values that the model was trained with
 * @param p Autoregressive order
 * @param d Differencing order
 * @param q Moving average order
 * @param includeIntercept If true, the model was fit with an intercept.
 * @param method Objective function and optimization method the model was trained wtih.
 * @param initParams User provided initial parameters for optimization.
 * @param hasIntercept True if the trained model has an intercept
 * @param coefficients Coefficients from the trained model
 */
case class ArimaModelTkMetaData(ts: DenseVector,
                                p: Int,
                                d: Int,
                                q: Int,
                                includeIntercept: Boolean,
                                method: String,
                                initParams: Option[Seq[Double]],
                                hasIntercept: Boolean,
                                coefficients: Array[Double]) extends Serializable

case class ARIMAData(arimaModel: SparkTsArimaModel, tsValues: List[Double]) {
  require(arimaModel != null, "arimaModel must not be null.")
  require(tsValues != null && tsValues.nonEmpty, "tsValues must not be null or empty.")
}

