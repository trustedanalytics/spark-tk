package org.trustedanalytics.sparktk.models.regression.linear_regression

import org.apache.spark.SparkContext
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.Row
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.trustedanalytics.sparktk.frame._
import org.trustedanalytics.sparktk.models.FrameRddFunctions
import org.trustedanalytics.sparktk.saveload.{ SaveLoad, TkSaveLoad, TkSaveableObject }
import org.apache.spark.ml.regression.{ LinearRegressionModel => SparkLinearRegressionModel }
import scala.collection.mutable.ListBuffer
import org.trustedanalytics.sparktk.frame.DataTypes.DataType
import org.apache.commons.lang.StringUtils

import scala.language.implicitConversions
import org.json4s.JsonAST.JValue

object LinearRegressionModel extends TkSaveableObject {

  /**
   * Run Spark Ml's LinearRegression on the training frame and create a Model for it.
   *
   * @param frame A frame to train the model on
   * @param valueColumn Column name containing the value for each observation.
   * @param observationColumns List of column(s) containing the observations.
   * @param elasticNetParameter Parameter for the ElasticNet mixing. Default is 0.0
   * @param fitIntercept Parameter for whether to fit an intercept term. Default is true
   * @param maxIterations Parameter for maximum number of iterations. Default is 100
   * @param regParam Parameter for regularization. Default is 0.0
   * @param standardization Parameter for whether to standardize the training features before fitting the model. Default is true
   * @param tolerance Parameter for the convergence tolerance for iterative algorithms. Default is 1E-6
   * @return returns LinearRegressionModel
   */
  def train(frame: Frame,
            valueColumn: String,
            observationColumns: Seq[String],
            elasticNetParameter: Double = 0.0,
            fitIntercept: Boolean = true,
            maxIterations: Int = 100,
            regParam: Double = 0.0,
            standardization: Boolean = true,
            tolerance: Double = 1E-6): LinearRegressionModel = {

    implicit def frameToFrameRddFunctions(frame: FrameRdd): FrameRddFunctions = {
      new FrameRddFunctions(frame)
    }

    require(frame != null, "frame is required")
    require(observationColumns != null && observationColumns.nonEmpty, "observationColumn must not be null nor empty")
    require(StringUtils.isNotEmpty(valueColumn), "valueColumn must not be null nor empty")
    require(maxIterations > 0, "numIterations must be a positive value")
    require(regParam >= 0, "regParam should be greater than or equal to 0")

    val trainFrameRdd = new FrameRdd(frame.schema, frame.rdd)
    val dataFrame = trainFrameRdd.toLabeledDataFrame(trainFrameRdd, valueColumn, observationColumns.toList)

    val linReg = new LinearRegression()
    linReg.setElasticNetParam(elasticNetParameter)
      .setFitIntercept(fitIntercept)
      .setMaxIter(maxIterations)
      .setRegParam(regParam)
      .setStandardization(standardization)
      .setTol(tolerance)
      .setLabelCol("label")
      .setFeaturesCol("features")

    val linRegModel = linReg.fit(dataFrame)

    linRegModel.setFeaturesCol("features")
    linRegModel.setPredictionCol("predicted_value")

    LinearRegressionModel(valueColumn,
      observationColumns,
      linRegModel.intercept,
      linRegModel.weights.toArray.toSeq,
      linRegModel.summary.explainedVariance,
      linRegModel.summary.meanAbsoluteError,
      linRegModel.summary.meanSquaredError,
      linRegModel.summary.objectiveHistory.toSeq,
      linRegModel.summary.r2,
      linRegModel.summary.rootMeanSquaredError,
      linRegModel.summary.totalIterations,
      linRegModel)
  }

  //Revisit with spark 1.6
  def load(sc: SparkContext, path: String, formatVersion: Int, tkMetadata: JValue): Any = {
    validateFormatVersion(formatVersion, 1)
    throw new RuntimeException("Load method yet to be implemented..!!")
  }

}

/**
 *
 * @param valueColumn Frame's column storing the value of the observation
 * @param observationColumnsTrain Frame's column(s) storing the observations
 * @param intercept The intercept of the trained model
 * @param weights Weights of the trained model
 * @param explainedVariance The explained variance regression score
 * @param meanAbsoluteError The risk function corresponding to the expected value of the absolute error loss or l1-norm loss
 * @param meanSquaredError The risk function corresponding to the expected value of the squared error loss or quadratic loss
 * @param objectiveHistory Objective function(scaled loss + regularization) at each iteration
 * @param r2 The coefficient of determination of the trained model
 * @param rootMeanSquaredError The square root of the mean squared error
 * @param iterations The number of training iterations until termination
 * @param sparkModel spark ml linear regression model
 */
case class LinearRegressionModel(valueColumn: String,
                                 observationColumnsTrain: Seq[String],
                                 intercept: Double,
                                 weights: Seq[Double],
                                 explainedVariance: Double,
                                 meanAbsoluteError: Double,
                                 meanSquaredError: Double,
                                 objectiveHistory: Seq[Double],
                                 r2: Double,
                                 rootMeanSquaredError: Double,
                                 iterations: Int,
                                 sparkModel: SparkLinearRegressionModel) extends Serializable {

  implicit def frameToFrameRddFunctions(frame: FrameRdd): FrameRddFunctions = {
    new FrameRddFunctions(frame)
  }

  /**
   * Predict values for a frame using a trained Linear Regression model
   *
   * @param frame The frame to predict on
   * @param observationColumns List of column(s) containing the observations
   * @return returns predicted frame
   */
  def predict(frame: Frame, observationColumns: Option[List[String]]): Frame = {

    require(frame != null, "require frame to predict")

    val predictFrameRdd = new FrameRdd(frame.schema, frame.rdd)

    val dataFrame = predictFrameRdd.toLabeledDataFrame(observationColumns.getOrElse(observationColumnsTrain.toList))

    val fullPrediction = sparkModel.transform(dataFrame)
    val prediction = fullPrediction.select("predicted_value").map(_.getDouble(0))
    val combinedRdd = predictFrameRdd.zip(prediction)

    val resultRdd: RDD[Row] = combinedRdd.map { value =>
      val row = value._1
      val label = value._2
      new GenericRow(row.toSeq.toArray :+ label)
    }

    var columnNames = new ListBuffer[String]()
    var columnTypes = new ListBuffer[DataTypes.DataType]()
    columnNames += "predicted_value"
    columnTypes += DataTypes.float64

    val newColumns = columnNames.toList.zip(columnTypes.toList.map(x => x: DataType))
    val updatedSchema = frame.schema.addColumns(newColumns.map { case (name, dataType) => Column(name, dataType) })

    new Frame(resultRdd, updatedSchema)
  }

  /**
   * Saves this model to a file
   * @param sc active SparkContext
   * @param path save to path
   */
  def save(sc: SparkContext, path: String): Unit = {
    throw new RuntimeException("Save method yet to be implemented..!!")
  }
}

/**
 * @param valueColumn Frame's column storing the value of the observation
 * @param observationColumns Frame's column(s) storing the observations
 * @param intercept The intercept of the trained model
 * @param weights Weights of the trained model
 * @param explainedVariance The explained variance regression score
 * @param meanAbsoluteError The risk function corresponding to the expected value of the absolute error loss or l1-norm loss
 * @param meanSquaredError The risk function corresponding to the expected value of the squared error loss or quadratic loss
 * @param objectiveHistory Objective function(scaled loss + regularization) at each iteration
 * @param r2 The coefficient of determination of the trained model
 * @param rootMeanSquaredError The square root of the mean squared error
 * @param iterations The number of training iterations until termination
 */
case class LinearRegressionModelMetaData(valueColumn: String,
                                         observationColumns: Seq[String],
                                         intercept: Double,
                                         weights: Array[Double],
                                         explainedVariance: Double,
                                         meanAbsoluteError: Double,
                                         meanSquaredError: Double,
                                         objectiveHistory: Array[Double],
                                         r2: Double,
                                         rootMeanSquaredError: Double,
                                         iterations: Int) extends Serializable