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
package org.trustedanalytics.sparktk.models.survivalanalysis.cox_ph

import java.nio.file.{ Files, Path }

import org.apache.commons.io.FileUtils
import org.apache.spark.SparkContext
import org.apache.spark.ml.regression.org.trustedanalytics.sparktk.{ CoxPh, CoxPhModel }
import org.apache.spark.mllib.linalg.{ DenseVector, VectorUDT }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.{ DataFrame, Row, SQLContext }
import org.apache.spark.sql.types.{ DoubleType, StructField, StructType }
import org.json4s.JsonAST.JValue
import org.trustedanalytics.scoring.interfaces.{ Field, Model, ModelMetaData }
import org.trustedanalytics.sparktk.TkContext
import org.trustedanalytics.sparktk.frame._
import org.trustedanalytics.sparktk.frame.internal.RowWrapper
import org.trustedanalytics.sparktk.frame.internal.rdd.{ FrameRdd, RowWrapperFunctions }
import org.trustedanalytics.sparktk.models.{ ScoringModelUtils, SparkTkModelAdapter }
import org.trustedanalytics.sparktk.saveload.{ SaveLoad, TkSaveLoad, TkSaveableObject }

import scala.language.implicitConversions

object CoxProportionalHazardsModel extends TkSaveableObject {

  /**
   * Fits Cox hazard function and creates a model for it.
   * @param frame: (Frame) A frame to train the model on
   * @param timeColumn: (str) Column name containing the time of occurence of each observation.
   * @param covariateColumns: (Seq[str]) List of column(s) containing the covariates.
   * @param censorColumn: (str) Column name containing censor value of each observation.
   * @param convergenceTolerance: (str) Parameter for the convergence tolerance for iterative algorithms. Default is 1E-6
   * @param maxSteps: (int) Parameter for maximum number of steps. Default is 100
   * @return (SparktkCoxPhModel) A trained coxPh model
   */
  def train(frame: Frame,
            timeColumn: String,
            covariateColumns: Seq[String],
            censorColumn: String,
            convergenceTolerance: Double = 1E-6,
            maxSteps: Int = 100) = {
    require(frame != null, "frame is required")
    require(timeColumn != null && timeColumn.nonEmpty, "Time column must not be null or empty")
    require(censorColumn != null && censorColumn.nonEmpty, "Censor column must not be null or empty")
    require(covariateColumns != null && covariateColumns.nonEmpty, "Covariate columns must not be null or empty")
    require(maxSteps > 0, "Max steps must be a positive integer")

    val arguments = CoxPhTrainArgs(frame,
      timeColumn,
      covariateColumns,
      censorColumn,
      convergenceTolerance,
      maxSteps)

    // Use DataFrames to run the coxPh
    val rdd: FrameRdd = new FrameRdd(frame.schema, frame.rdd)
    val trainFrame = toCoxDataFrame(rdd, covariateColumns, timeColumn, censorColumn)
    val cox = new CoxPh()
    cox.setLabelCol("time")
    cox.setFeaturesCol("features")
    cox.setCensorCol("censor")
    cox.setMaxIter(arguments.maxSteps)
    cox.setTol(arguments.convergenceTolerance)
    val coxModel: CoxPhModel = cox.fit(trainFrame)

    CoxProportionalHazardsModel(coxModel,
      timeColumn,
      covariateColumns,
      censorColumn,
      convergenceTolerance,
      maxSteps,
      coxModel.beta.toArray.toList,
      coxModel.meanVector.toArray.toList
    )
  }

  /**
   * Convert FrameRdd to DataFrame with features of type Vector, time of type double and censor of type double
   * @param featureColumnNames: (Seq[String]) List of covariate column names
   * @param timeColumn: (String) Name of column containing time
   * @param censorColumn (String) Name of column containing censor values
   * @return DataFrame with features, time and censor
   */
  def toCoxDataFrame(frameRDD: FrameRdd, featureColumnNames: Seq[String], timeColumn: String, censorColumn: String): DataFrame = {
    val rdd: RDD[(DenseVector, Double, Double)] = frameRDD.mapRows(row => {
      val features = row.valuesAsDoubleArray(featureColumnNames)
      (new DenseVector(features),
        DataTypes.toDouble(row.value(timeColumn)),
        DataTypes.toDouble(row.value(censorColumn)))
    })
    val rowRdd: RDD[Row] = rdd.map(entry => new GenericRow(Array[Any](entry._1, entry._2, entry._3)))
    val schema = StructType(Seq(StructField("features", new VectorUDT, true),
      StructField("time", DoubleType, true), StructField("censor", DoubleType, true)))
    new SQLContext(frameRDD.sparkContext).createDataFrame(rowRdd, schema)
  }

  /**
   * Load method where the work of getting the formatVersion and tkMetadata has already been done
   * @param sc            active spark context
   * @param path          the source path
   * @param formatVersion the version of the format for the tk metadata that should be recorded.
   * @param tkMetadata    the data to save (should be a case class), must be serializable to JSON using json4s
   * @return loaded object
   */
  override def loadTkSaveableObject(sc: SparkContext, path: String, formatVersion: Int, tkMetadata: JValue): Any = {
    validateFormatVersion(formatVersion, 1)
    val coxPhMetadata: CoxPhMetaData = SaveLoad.extractFromJValue[CoxPhMetaData](tkMetadata)
    val sparkModel: CoxPhModel = CoxPhModel.read.load(path)

    CoxProportionalHazardsModel(sparkModel,
      coxPhMetadata.timeColumn,
      coxPhMetadata.covariateColumns,
      coxPhMetadata.censorColumn,
      coxPhMetadata.convergenceTolerance,
      coxPhMetadata.maxSteps,
      sparkModel.beta.toArray.toList,
      sparkModel.meanVector.toArray.toList)
  }

  /**
   * Load a SparktkCoxPhModel from the given path
   * @param tc TkContext
   * @param path location
   * @return loaded object
   */
  def load(tc: TkContext, path: String): CoxProportionalHazardsModel = {
    tc.load(path).asInstanceOf[CoxProportionalHazardsModel]
  }
}

/**
 * SparktkCoxPhModel class uses a trained CoxPhModel object to predict, score, etc.
 * @param sparkModel: Trained CoxPhModel
 * @param timeColumn: (str) Column name containing the time of occurence of each observation.
 * @param covariateColumns: (Seq[str]) List of column(s) containing the covariates.
 * @param censorColumn: (str) Column name containing censor value of each observation.
 * @param convergenceTolerance: (str) Parameter for the convergence tolerance for iterative algorithms. Default is 1E-6
 * @param maxSteps: (int) Parameter for maximum number of steps. Default is 100
 * @param beta: (List[Double]) Trained beta values for each column
 * @param mean: (List[Double]) Mean of each column
 */
case class CoxProportionalHazardsModel private[cox_ph] (sparkModel: CoxPhModel,
                                                        timeColumn: String,
                                                        covariateColumns: Seq[String],
                                                        censorColumn: String,
                                                        convergenceTolerance: Double,
                                                        maxSteps: Int,
                                                        beta: List[Double],
                                                        mean: List[Double]) extends Serializable with Model {
  implicit def rowWrapperToRowWrapperFunctions(rowWrapper: RowWrapper): RowWrapperFunctions = {
    new RowWrapperFunctions(rowWrapper)
  }

  /**
   * Predict values for a frame using a trained Cox proportional hazards model
   * @param frame The frame to predict on
   * @param observationColumns List of column(s) containing the observations
   * @param comparisonFrame The frame to compare with
   * @return returns predicted frame
   */
  def predict(frame: Frame, observationColumns: Option[Seq[String]], comparisonFrame: Option[Frame]): Frame = {

    require(frame != null, "require frame to predict")

    val featureColumns = observationColumns.getOrElse(covariateColumns)
    val meanVector = if (comparisonFrame.isDefined) {
      val compareFrame: Frame = comparisonFrame.get
      new FrameRdd(compareFrame.schema, compareFrame.rdd).columnStatistics(featureColumns).mean
    }
    else {
      sparkModel.meanVector
    }
    val hazardRatioColumn = Column("hazard_ratio", DataTypes.float64)
    val predictFrame = new FrameRdd(frame.schema, frame.rdd).addColumn(hazardRatioColumn, row => {
      val observation = row.valuesAsDenseVector(featureColumns)
      sparkModel.predict(observation, meanVector)
    })
    new Frame(predictFrame.rdd, predictFrame.schema)
  }
  /**
   * Saves this model to a file
   * @param sc active SparkContext
   * @param path save to path
   * @param overwrite Boolean indicating if the directory will be overwritten, if it already exists.
   */
  def save(sc: SparkContext, path: String, overwrite: Boolean = false): Unit = {

    if (overwrite)
      sparkModel.write.overwrite().save(path)
    else
      sparkModel.write.save(path)
    val formatVersion: Int = 1
    val tkMetadata = CoxPhMetaData(timeColumn,
      covariateColumns,
      censorColumn,
      convergenceTolerance,
      maxSteps,
      sparkModel.beta.toArray.toList,
      sparkModel.meanVector.toArray.toList)

    TkSaveLoad.saveTk(sc, path, CoxProportionalHazardsModel.formatId, formatVersion, tkMetadata)
  }

  /**
   * Scores the input array against the trained model
   * @param row: (Array[Any]) Array of input data that needs to be scored
   * @return hazard ratio score
   */
  override def score(row: Array[Any]): Array[Any] = {
    require(row != null && row.length > 0, "scoring input row must not be null nor empty")
    val doubleArray = row.map(i => ScoringModelUtils.asDouble(i))
    val hazard_ratio = sparkModel.predict(new DenseVector(doubleArray), sparkModel.meanVector)
    row :+ hazard_ratio
  }

  /**
   * Exports the model to the given path on hdfs
   * @param sc: SparkContext
   * @param marSavePath: hdfs path where the model needs to be exported
   * @return hdfs path where the model was exported
   */
  def exportToMar(sc: SparkContext, marSavePath: String): String = {
    var tmpDir: Path = null
    try {
      tmpDir = Files.createTempDirectory("sparktk-scoring-model")
      save(sc, tmpDir.toString, overwrite = true)
      ScoringModelUtils.saveToMar(marSavePath, classOf[CoxProportionalHazardsModel].getName, tmpDir)
    }
    finally {
      sys.addShutdownHook(FileUtils.deleteQuietly(tmpDir.toFile)) // Delete temporary directory on exit
    }
  }

  /**
   * @return Metadata of the current model
   */
  def modelMetadata(): ModelMetaData = {
    new ModelMetaData("CoxPH Model", classOf[CoxProportionalHazardsModel].getName, classOf[SparkTkModelAdapter].getName, Map())
  }

  override def input(): Array[Field] = {
    val obsCols = covariateColumns
    var input = Array[Field]()
    obsCols.foreach { name =>
      input = input :+ Field(name, "Double")
    }
    input
  }

  override def output(): Array[Field] = {
    var output = input()
    output :+ Field("hazard_ratio", "Double")
  }

}

/**
 * CoxPhMetaData contains metadata of the trained model
 * @param timeColumn: (str) Column name containing the time of occurence of each observation.
 * @param covariateColumns: (Seq[str]) List of column(s) containing the covariates.
 * @param censorColumn: (str) Column name containing censor value of each observation.
 * @param convergenceTolerance: (float) Parameter for the convergence tolerance for iterative algorithms. Default is 1E-6
 * @param maxSteps: (int) Parameter for maximum number of steps. Default is 100
 * @param beta: (List[Double]) List of trained beta values for each covariate
 * @param mean: (List[Double]) List of means of each covariate column
 */
case class CoxPhMetaData(timeColumn: String,
                         covariateColumns: Seq[String],
                         censorColumn: String,
                         convergenceTolerance: Double,
                         maxSteps: Int,
                         beta: List[Double],
                         mean: List[Double]) extends Serializable

/**
 * CoxPhTrainArgs contains parameters for train method
 * @param frame: (Frame) A frame to train the model on
 * @param timeColumn: (str) Column name containing the time of occurence of each observation.
 * @param covariateColumns: (Seq[str]) List of column(s) containing the covariates.
 * @param censorColumn: (str) Column name containing censor value of each observation.
 * @param convergenceTolerance: (float) Parameter for the convergence tolerance for iterative algorithms. Default is 1E-6
 * @param maxSteps: (int) Parameter for maximum number of steps. Default is 100
 */

case class CoxPhTrainArgs(frame: Frame,
                          timeColumn: String,
                          covariateColumns: Seq[String],
                          censorColumn: String,
                          convergenceTolerance: Double,
                          maxSteps: Int)