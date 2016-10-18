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
package org.trustedanalytics.sparktk.models.timeseries.max

import com.cloudera.sparkts.models.{ ARIMAX => SparkTsMax, ARIMAXModel => SparkTsMaxModel }
import org.apache.commons.lang.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.sql.Row
import org.json4s.JsonAST.JValue
import org.trustedanalytics.sparktk.TkContext
import breeze.linalg.{ DenseVector => BreezeDenseVector, DenseMatrix => BreezeDenseMatrix }
import org.trustedanalytics.sparktk.frame.internal.ops.timeseries.TimeSeriesFunctions
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.trustedanalytics.sparktk.frame.{ Column, DataTypes, Frame }
import org.trustedanalytics.sparktk.saveload.{ SaveLoad, TkSaveLoad, TkSaveableObject }
import org.trustedanalytics.scoring.interfaces.{ ModelMetaData, Field, Model }
import org.trustedanalytics.sparktk.models.{ SparkTkModelAdapter, ScoringModelUtils }
import java.nio.file.{ Files, Path }
import org.apache.commons.io.FileUtils

object MaxModel extends TkSaveableObject {

  /**
   * Current format version for model save/load
   */
  private val currentFormatVersion: Int = 1

  /**
   * List of format version that are valid for loading
   */
  private val validFormatVersions = List[Int](currentFormatVersion)

  /**
   * Creates Moving Average with Exogeneous Variables (MAx) model from the specified time-series values.
   *
   * Given a time series, fits Moving Average with Exogeneous Variables (MAx) model. Q represents the moving average
   * error terms, xregMaxLag represents the maximum lag order for exogenous variables, includeOriginalXreg is a boolean
   * flag indicating if the non-lagged exogenous variables should be included. If includeIntercept is true, the model
   * is fitted with an intercept. Exogenous Variables are represented by xColumns.
   *
   * If the specified set of exogenous variables is not invertible, an exception is thrown stating that
   * the "matrix is singular". This happens when there are certain patterns in the dataset or columns of all
   * zeros.  In order to work around the singular matrix issue, try selecting a different set of columns for
   * exogenous variables, or use a different time window for training.
   *
   * @param frame A frame to train the model on.
   * @param timeseriesColumn Name of the column that contains the time series values.
   * @param xColumns Names of the column(s) that contain the values of previous exogenous regressors.
   * @param q Moving average order
   * @param xregMaxLag The maximum lag order for exogenous variables.
   * @param includeOriginalXreg If true, the model is fit with an intercept for exogenous variables. Default is True.
   * @param includeIntercept If true, the model was fit with an intercept.
   * @param initParams A set of user provided initial parameters for optimization. If the list is empty
   *                   (default), initialized using Hannan-Rissanen algorithm. If provided, order of parameter
   *                   should be: intercept term, 0 (for autoregressive part), MA parameters
   *                   (in increasing order of lag).
   * @return The trained MAX model
   */
  def train(frame: Frame,
            timeseriesColumn: String,
            xColumns: Seq[String],
            q: Int,
            xregMaxLag: Int,
            includeOriginalXreg: Boolean = true,
            includeIntercept: Boolean = true,
            initParams: Option[Seq[Double]] = None): MaxModel = {

    require(frame != null, "frame is required")
    require(StringUtils.isNotEmpty(timeseriesColumn), "TimeseriesColumn must not be null nor empty")
    require(xColumns != null && xColumns.nonEmpty, "Must provide at least one x column.")

    val (p, d) = (0, 0)
    val trainFrameRdd = new FrameRdd(frame.schema, frame.rdd)
    trainFrameRdd.cache()

    val (yVector, xMatrix) = TimeSeriesFunctions.getYAndXFromFrame(trainFrameRdd, timeseriesColumn, xColumns)
    val ts = new DenseVector(yVector.toArray)

    val userInitParams: Option[Array[Double]] = if (initParams.isDefined) Some(initParams.get.toArray) else None

    val maxModel = SparkTsMax.fitModel(p, d, q, ts, xMatrix, xregMaxLag, includeOriginalXreg, includeIntercept, userInitParams)

    trainFrameRdd.unpersist()

    MaxModel(timeseriesColumn, xColumns, q, xregMaxLag, includeOriginalXreg, includeIntercept, initParams, maxModel)
  }

  /**
   * Load a MaxModel from the given path
   * @param tc TkContext
   * @param path location
   * @return
   */
  def load(tc: TkContext, path: String): MaxModel = {
    tc.load(path).asInstanceOf[MaxModel]
  }

  /**
   * Load model from file
   *
   * @param sc active spark context
   * @param path the source path
   * @param formatVersion the version of the format for the tk metadata that should be recorded.
   * @param tkMetadata the data to save (should be a case class), must be serializable to JSON using json4s
   * @return MaxModel loaded from the specified file
   */
  override def loadTkSaveableObject(sc: SparkContext, path: String, formatVersion: Int, tkMetadata: JValue): Any = {
    validateFormatVersion(formatVersion, validFormatVersions: _*)
    val m: MaxModelTkMetaData = SaveLoad.extractFromJValue[MaxModelTkMetaData](tkMetadata)

    // Define the spark-ts ARIMAX model
    val sparkModel = new SparkTsMaxModel(0, 0, m.q, m.xregMaxLag, m.coefficients, m.includeOriginalXreg, m.includeIntercept)

    // Create MaxModel to return
    MaxModel(m.timeseriesColumn, m.xColumns, m.q, m.xregMaxLag, m.includeOriginalXreg, m.includeIntercept, m.initParams, sparkModel)
  }
}

case class MaxModel private[max] (timeseriesColumn: String,
                                  xColumns: Seq[String],
                                  q: Int,
                                  xregMaxLag: Int,
                                  includeOriginalXreg: Boolean = true,
                                  includeIntercept: Boolean = true,
                                  initParams: Option[Seq[Double]],
                                  maxModel: SparkTsMaxModel) extends Serializable with Model {

  lazy val p: Int = 0

  lazy val d: Int = 0

  /**
   * An intercept term
   */
  lazy val c: Double = maxModel.coefficients(0)

  /**
   * Coefficient values: AR with increasing degrees
   */
  lazy val ar: Seq[Double] = maxModel.coefficients.slice(1, 1)

  /**
   * Coefficient values: MA with increasing degrees
   */
  lazy val ma: Seq[Double] = maxModel.coefficients.slice(1, q + 1)

  /**
   * Coefficient values: xreg with increasing degrees
   */
  lazy val xreg: Seq[Double] = maxModel.coefficients.drop(q + 1)

  def predict(frame: Frame,
              predictTimeseriesColumn: String,
              predictXColumns: Seq[String]): Frame = {
    require(frame != null, "Frame cannot be null.")
    require(predictXColumns.length == xColumns.length, "Number of columns for train and predict should be the same")

    // Get vector or y values and matrix of x values
    val predictFrameRdd = new FrameRdd(frame.schema, frame.rdd)
    val (yVector, xMatrix) = TimeSeriesFunctions.getYAndXFromFrame(predictFrameRdd, predictTimeseriesColumn, predictXColumns)

    // Find the maximum lag and fill that many spots with nulls, followed by the predicted y values
    val ts = new BreezeDenseVector(yVector.toArray)

    val predictions = maxModel.predict(ts, xMatrix).toArray
    val numPredictions = predictions.length

    val dataWithPredictions = frame.rdd.zipWithIndex().map {
      case (row: Row, index: Long) =>
        if (numPredictions > index)
          Row.fromSeq(row.toSeq :+ predictions(index.toInt))
        else
          Row.fromSeq(row.toSeq :+ null)
    }

    val schemaWithPredictions = frame.schema.addColumn(Column("predicted_y", DataTypes.float64))

    new Frame(dataWithPredictions, schemaWithPredictions)
  }
  /**
   * Saves trained model to the specified path
   *
   * @param sc spark context
   * @param path path to save file
   */
  def save(sc: SparkContext, path: String): Unit = {
    val tkMetadata = MaxModelTkMetaData(timeseriesColumn, xColumns, q, xregMaxLag, includeOriginalXreg, includeIntercept, maxModel.coefficients, initParams)
    TkSaveLoad.saveTk(sc, path, MaxModel.formatId, MaxModel.currentFormatVersion, tkMetadata)
  }

  override def score(data: Array[Any]): Array[Any] = {
    require(data != null && data.length > 0, "scoring data must not be null nor empty.")
    val xColumnsLength = xColumns.length
    var predictedValues = Array[Any]()

    // We should have an array of y values, and an array of x values
    if (data.length != 2)
      throw new IllegalArgumentException("Expected 2 arrays of data (for y values and x values), but received " +
        data.length.toString + " items.")

    val yValues = data(0) match {
      case yList: List[_] => new BreezeDenseVector(yList.map(ScoringModelUtils.asDouble(_)).toArray)
      case yArray: Array[_] => new BreezeDenseVector(yArray.map(ScoringModelUtils.asDouble(_)))
      case _ => throw new IllegalArgumentException("Expected first element in data array to be an Array[Double] of y values.")
    }
    val xArray = data(1) match {
      case xList: List[_] => xList.map(ScoringModelUtils.asDouble(_)).toArray
      case xArray: Array[_] => xArray.map(ScoringModelUtils.asDouble(_))
      case _ => throw new IllegalArgumentException("Expected second element in data array to be an Array[Double] of x values.")
    }

    if (xArray.length != (yValues.length * xColumnsLength))
      throw new IllegalArgumentException("Expected " + (yValues.length * xColumnsLength) + " x values, but received " +
        xArray.length.toString)

    val xValues = new BreezeDenseMatrix(rows = yValues.length, cols = xColumnsLength, data = xArray)

    data :+ maxModel.predict(yValues, xValues).toArray
  }

  override def modelMetadata(): ModelMetaData = {
    new ModelMetaData("MAX Model", classOf[MaxModel].getName, classOf[SparkTkModelAdapter].getName, Map())
  }

  override def input(): Array[Field] = {
    Array[Field](Field("y", "Array[Double]"), Field("x_values", "Array[Double]"))
  }

  override def output(): Array[Field] = {
    var output = input()
    output :+ Field("score", "Array[Double]")
  }

  def exportToMar(sc: SparkContext, marSavePath: String): String = {
    var tmpDir: Path = null
    try {
      tmpDir = Files.createTempDirectory("sparktk-scoring-model")
      save(sc, tmpDir.toString)
      ScoringModelUtils.saveToMar(marSavePath, classOf[MaxModel].getName, tmpDir)
    }
    finally {
      sys.addShutdownHook(FileUtils.deleteQuietly(tmpDir.toFile)) // Delete temporary directory on exit
    }
  }
}

/**
 * TK Metadata that will be stored as part of the MAX model
 *
 * @param timeseriesColumn Name of the column that contains the time series values.
 * @param xColumns Names of the column(s) that contain the values of previous exogenous regressors.
 * @param q Moving average order
 * @param xregMaxLag The maximum lag order for exogenous variables.
 * @param includeOriginalXreg If true, the model is fit with an intercept for exogenous variables. Default is True.
 * @param includeIntercept If true, the model is fit with an intercept. Default is True.
 * @param coefficients Coefficients from the trained model
 * @param initParams User provided initial parameters for optimization.
 */
case class MaxModelTkMetaData(timeseriesColumn: String,
                              xColumns: Seq[String],
                              q: Int,
                              xregMaxLag: Int,
                              includeOriginalXreg: Boolean,
                              includeIntercept: Boolean,
                              coefficients: Array[Double],
                              initParams: Option[Seq[Double]]) extends Serializable