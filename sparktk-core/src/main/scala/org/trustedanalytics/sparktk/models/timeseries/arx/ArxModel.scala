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
package org.trustedanalytics.sparktk.models.timeseries.arx

import breeze.linalg._
import com.cloudera.sparkts.models.{ ARXModel => SparkTsArxModel, AutoregressionX }
import org.apache.commons.lang.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.json4s.JsonAST.JValue
import org.trustedanalytics.sparktk.frame.internal.ops.timeseries.TimeSeriesFunctions
import org.trustedanalytics.sparktk.TkContext
import org.trustedanalytics.sparktk.frame.{ Column, DataTypes, Frame }
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.trustedanalytics.sparktk.saveload.{ SaveLoad, TkSaveLoad, TkSaveableObject }
import org.trustedanalytics.scoring.interfaces.{ ModelMetaData, Field, Model }
import org.trustedanalytics.sparktk.models.{ SparkTkModelAdapter, ScoringModelUtils }
import java.nio.file.{ Files, Path }
import org.apache.commons.io.FileUtils

object ArxModel extends TkSaveableObject {

  /**
   * Current format version for model save/load
   */
  private val currentFormatVersion: Int = 1

  /**
   * List of format version that are valid for loading
   */
  private val validFormatVersions = List[Int](currentFormatVersion)

  /**
   * A boolean flag indicating if the non-lagged exogenous variables should be included
   */
  private val includesOriginalX: Boolean = true

  /**
   * Fit an autoregressive model with additional exogenous variables.
   *
   * **Notes**
   *
   * #)  Dataset being trained must be small enough to be worked with on a single node.
   * #)  If the specified set of exogenous variables is not invertible, an exception is thrown stating that
   * the "matrix is singular".  This happens when there are certain patterns in the dataset or columns of all
   * zeros.  In order to work around the singular matrix issue, try selecting a different set of columns for
   * exogenous variables, or use a different time window for training.
   *
   * @param frame A frame to train the model on.
   * @param timeseriesColumn Name of the column that contains the time series values.
   * @param xColumns Names of the column(s) that contain the values of previous exogenous regressors.
   * @param yMaxLag The maximum lag order for the dependent (time series) variable.
   * @param xMaxLag The maximum lag order for exogenous variables.
   * @param noIntercept A boolean flag indicating if the intercept should be dropped. Default is false.
   * @return The trained ARX model
   */
  def train(frame: Frame,
            timeseriesColumn: String,
            xColumns: Seq[String],
            yMaxLag: Int,
            xMaxLag: Int,
            noIntercept: Boolean = false): ArxModel = {
    require(frame != null, "frame is required")
    require(StringUtils.isNotEmpty(timeseriesColumn), "timeseriesColumn must not be null nor empty")
    require(xColumns != null && xColumns.nonEmpty, "Must provide at least one x column.")

    val trainFrameRdd = new FrameRdd(frame.schema, frame.rdd)
    trainFrameRdd.cache()
    val (yVector, xMatrix) = TimeSeriesFunctions.getYAndXFromFrame(trainFrameRdd, timeseriesColumn, xColumns)
    val arxModel = AutoregressionX.fitModel(yVector, xMatrix, yMaxLag, xMaxLag, includesOriginalX, noIntercept)
    trainFrameRdd.unpersist()

    ArxModel(timeseriesColumn, xColumns, yMaxLag, xMaxLag, noIntercept, arxModel)
  }

  /**
   * Load a ArxModel from the given path
   * @param tc TkContext
   * @param path location
   * @return
   */
  def load(tc: TkContext, path: String): ArxModel = {
    tc.load(path).asInstanceOf[ArxModel]
  }

  /**
   * Load model from file
   *
   * @param sc active spark context
   * @param path the source path
   * @param formatVersion the version of the format for the tk metadata that should be recorded.
   * @param tkMetadata the data to save (should be a case class), must be serializable to JSON using json4s
   * @return ARX Model loaded from the specified file.
   */
  override def loadTkSaveableObject(sc: SparkContext, path: String, formatVersion: Int, tkMetadata: JValue): Any = {
    validateFormatVersion(formatVersion, validFormatVersions: _*)
    val m: ArxModelTkMetaData = SaveLoad.extractFromJValue[ArxModelTkMetaData](tkMetadata)

    // Define the spark-ts ARX model
    val sparkModel = new SparkTsArxModel(m.c, m.coefficients, m.yMaxLag, m.xMaxLag, includesOriginalX)

    // Create the ArxModel to return
    ArxModel(m.timeseriesColumn, m.xColumns, m.yMaxLag, m.xMaxLag, m.noIntercept, sparkModel)
  }
}

case class ArxModel private[arx] (timeseriesColumn: String,
                                  xColumns: Seq[String],
                                  yMaxLag: Int,
                                  xMaxLag: Int,
                                  noIntercept: Boolean = false,
                                  arxModel: SparkTsArxModel) extends Serializable with Model {

  /**
   * An intercept term, zero if none desired
   */
  def c: Double = arxModel.c

  /**
   * The coefficients for the various terms. The order of coefficients is as follows:
   *     - Autoregressive terms for the dependent variable, in increasing order of lag
   *     - For each column in the exogenous matrix (in their original order), the lagged terms in
   *       increasing order of lag (excluding the non-lagged versions).
   *     - The coefficients associated with the non-lagged exogenous matrix
   */
  def coefficients: Seq[Double] = arxModel.coefficients

  /**
   * Predict the time series values for a test frame, based on the specified x values.  Creates a new frame revision
   * with the existing columns and a new predicted_y column.
   *
   * @param frame A frame whose values are to be predicted.
   * @param predictTimeseriesColumn Name of the column that contains the time series values.
   * @param predictXColumns Names of the column(s) that contain the values of the exogenous inputs.
   * @return A new frame containing the original frame's columns and a column *predictied_y*
   */
  def predict(frame: Frame,
              predictTimeseriesColumn: String,
              predictXColumns: Seq[String]): Frame = {
    require(frame != null, "Frame cannot be null.")
    require(predictXColumns.length == xColumns.length, "Number of columns for train and predict should be the same")

    // Get vector or y values and matrix of x values
    val predictFrameRdd = new FrameRdd(frame.schema, frame.rdd)
    val (yVector, xMatrix) = TimeSeriesFunctions.getYAndXFromFrame(predictFrameRdd, predictTimeseriesColumn, predictXColumns)

    // Find the maximum lag and fill that many spots with nulls, followed by the predicted y values
    val maxLag = max(arxModel.yMaxLag, arxModel.xMaxLag)
    val predictions = Array.fill(maxLag) { null } ++ arxModel.predict(yVector, xMatrix).toArray
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
    val tkMetadata = ArxModelTkMetaData(timeseriesColumn, xColumns, yMaxLag, xMaxLag, noIntercept, arxModel.c, arxModel.coefficients)
    TkSaveLoad.saveTk(sc, path, ArxModel.formatId, ArxModel.currentFormatVersion, tkMetadata)
  }

  override def score(data: Array[Any]): Array[Any] = {
    require(data != null && data.length > 0, "scoring data must not be null nor empty")
    val xColumnsLength = xColumns.length
    var predictedValues = Array[Any]()

    // We should have an array of y values, and an array of x values
    if (data.length != 2)
      throw new IllegalArgumentException("Expected 2 arrays of data (for y values and x values), but received " +
        data.length.toString + " items.")

    val yValues = data(0) match {
      case a: Array[_] => new DenseVector(a.map(ScoringModelUtils.asDouble(_)))
      case l: List[_] => new DenseVector(l.map(ScoringModelUtils.asDouble(_)).toArray)
      case _ => throw new IllegalArgumentException(s"Expected first element in data array to be an Array[Double] of y values, but found ${data(0).getClass.getSimpleName}.")
    }
    val xArray = data(1) match {
      case a: Array[_] => a.map(ScoringModelUtils.asDouble(_))
      case l: List[_] => l.map(ScoringModelUtils.asDouble(_)).toArray
      case _ => throw new IllegalArgumentException(s"Expected second element in data array to be an Array[Double] of x values, but found ${data(1).getClass.getSimpleName}.")
    }

    if (xArray.length != (yValues.length * xColumnsLength))
      throw new IllegalArgumentException("Expected " + (yValues.length * xColumnsLength) + " x values, but received " +
        xArray.length.toString)

    val xValues = new DenseMatrix(rows = yValues.length, cols = xColumnsLength, data = xArray)

    data :+ arxModel.predict(yValues, xValues).toArray
  }

  override def modelMetadata(): ModelMetaData = {
    new ModelMetaData("ARX Model", classOf[ArxModel].getName, classOf[SparkTkModelAdapter].getName, Map())
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
      save(sc, "file://" + tmpDir.toString)
      ScoringModelUtils.saveToMar(marSavePath, classOf[ArxModel].getName, tmpDir)
    }
    finally {
      sys.addShutdownHook(FileUtils.deleteQuietly(tmpDir.toFile)) // Delete temporary directory on exit
    }
  }
}

/**
 * TK Metadata that will be stored as part of the ARX model
 *
 * @param timeseriesColumn Name of the column that contains the time series values.
 * @param xColumns Names of the column(s) that contain the values of previous exogenous regressors.
 * @param yMaxLag The maximum lag order for the dependent (time series) variable.
 * @param xMaxLag The maximum lag order for exogenous variables.
 * @param noIntercept A boolean flag indicating if the intercept should be dropped.
 * @param c an intercept term, zero if none desired, from the trained model
 * @param coefficients coefficient values from the trained model
 */
case class ArxModelTkMetaData(timeseriesColumn: String,
                              xColumns: Seq[String],
                              yMaxLag: Int,
                              xMaxLag: Int,
                              noIntercept: Boolean,
                              c: Double,
                              coefficients: Array[Double]) extends Serializable