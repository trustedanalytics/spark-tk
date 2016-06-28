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

package org.trustedanalytics.sparktk.models.timeseries.arx

import breeze.linalg._
import com.cloudera.sparkts.models.{ ARXModel => SparkTsArxModel, AutoregressionX }
import org.apache.commons.lang.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.json4s.{ DefaultFormats, Extraction }
import org.json4s.JsonAST.JValue
import org.trustedanalytics.sparktk.frame.{ Column, DataTypes, Frame }
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.trustedanalytics.sparktk.saveload.{ SaveLoad, TkSaveLoad, TkSaveableObject }

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
    val (yVector, xMatrix) = getYandXFromFrame(trainFrameRdd, timeseriesColumn, xColumns)
    val arxModel = AutoregressionX.fitModel(yVector, xMatrix, yMaxLag, xMaxLag, includesOriginalX, noIntercept)
    trainFrameRdd.unpersist()

    ArxModel(timeseriesColumn, xColumns, yMaxLag, xMaxLag, noIntercept, arxModel)
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
  override def load(sc: SparkContext, path: String, formatVersion: Int, tkMetadata: JValue): Any = {
    validateFormatVersion(formatVersion, validFormatVersions: _*)
    val m: ArxModelTkMetaData = SaveLoad.extractFromJValue[ArxModelTkMetaData](tkMetadata)

    // Define the spark-ts ARX model
    val sparkModel = new SparkTsArxModel(m.c, m.coefficients, m.yMaxLag, m.xMaxLag, includesOriginalX)

    // Create the ArxModel to return
    ArxModel(m.timeseriesColumn, m.xColumns, m.yMaxLag, m.xMaxLag, m.noIntercept, sparkModel)
  }

  /**
   * Gets values from the specified y and x columns.
   * @param frame Frame to get values from
   * @param yColumnName Name of the y column
   * @param xColumnNames Name of the x columns
   * @return Array of y values, and 2-dimensional array of x values
   */
  private[arx] def getYandXFromRows(frame: FrameRdd, yColumnName: String, xColumnNames: Seq[String]): (Array[Double], Array[Array[Double]]) = {
    val schema = frame.frameSchema

    schema.requireColumnIsNumerical(yColumnName)
    xColumnNames.foreach((xColumn: String) => schema.requireColumnIsNumerical(xColumn))

    val totalRowCount = frame.count.toInt
    val yValues = new Array[Double](totalRowCount)
    val xValues = Array.ofDim[Double](totalRowCount, xColumnNames.size)
    var rowCounter = 0
    val yColumnIndex = schema.columnIndex(yColumnName)

    for (row <- frame.collect()) {
      yValues(rowCounter) = DataTypes.toDouble(row.get(yColumnIndex))

      var xColumnCounter = 0
      for (xColumn <- xColumnNames) {
        xValues(rowCounter)(xColumnCounter) = DataTypes.toDouble(row.get(schema.columnIndex(xColumn)))
        xColumnCounter += 1
      }

      rowCounter += 1
    }

    (yValues, xValues)

  }

  /**
   * Gets x and y values from the specified frame
   * @param frame  Frame to get values from
   * @param yColumnName Name of the column that has y values
   * @param xColumnNames Name of the columns that have x values
   * @return Vector of y values and Matrix of x values
   */
  private[arx] def getYandXFromFrame(frame: FrameRdd, yColumnName: String, xColumnNames: Seq[String]): (Vector[Double], Matrix[Double]) = {

    // Get values in arrays
    val (yValues, xValues) = getYandXFromRows(frame, yColumnName, xColumnNames)

    // Put values into a vector and matrix to return
    val yVector = new DenseVector(yValues)
    val xMatrix = new DenseMatrix(rows = yValues.length, cols = xColumnNames.size, data = xValues.transpose.flatten)

    (yVector, xMatrix)
  }
}

case class ArxModel private[arx] (timeseriesColumn: String,
                                  xColumns: Seq[String],
                                  yMaxLag: Int,
                                  xMaxLag: Int,
                                  noIntercept: Boolean = false,
                                  arxModel: SparkTsArxModel) extends Serializable {

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
    val (yVector, xMatrix) = ArxModel.getYandXFromFrame(predictFrameRdd, predictTimeseriesColumn, predictXColumns)

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