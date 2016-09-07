/**
 * Copyright (c) 2015 Intel Corporation 
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.trustedanalytics.sparktk.models.timeseries.arima

import org.apache.commons.lang.StringUtils
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.SparkContext
import org.json4s.DefaultFormats
import org.json4s.JsonAST.JValue
import org.trustedanalytics.sparktk.TkContext
import org.trustedanalytics.sparktk.saveload.{ SaveLoad, TkSaveLoad, TkSaveableObject }
import com.cloudera.sparkts.models.{ ARIMA => SparkTsArima, ARIMAModel => SparkTsArimaModel }

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

case class ArimaModel private[arima] (ts: DenseVector,
                                      p: Int,
                                      d: Int,
                                      q: Int,
                                      includeIntercept: Boolean,
                                      method: String,
                                      initParams: Option[Seq[Double]],
                                      arimaModel: SparkTsArimaModel) extends Serializable {

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
