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
package org.trustedanalytics.sparktk.models.survivalanalysis

import breeze.linalg.DenseMatrix
import org.apache.spark.SparkContext
import org.apache.spark.ml.regression.org.trustedanalytics.sparktk.{ CoxPh, CoxPhModel }
import org.apache.spark.sql.{ DataFrame, Row }
import org.json4s.JsonAST.JValue
import org.trustedanalytics.sparktk.TkContext
import org.trustedanalytics.sparktk.frame._
import org.trustedanalytics.sparktk.frame.internal.RowWrapper
import org.trustedanalytics.sparktk.frame.internal.rdd.{ FrameRdd, RowWrapperFunctions, ScoreAndLabel }
import org.trustedanalytics.sparktk.saveload.{ SaveLoad, TkSaveLoad, TkSaveableObject }

import scala.language.implicitConversions
import org.trustedanalytics.scoring.interfaces.{ Field, Model, ModelMetaData }
import org.apache.spark.mllib.linalg.DenseVector
import org.trustedanalytics.sparktk.models.{ ScoringModelUtils, SparkTkModelAdapter }
import java.nio.file.{ Files, Path }

import org.apache.commons.io.FileUtils

object SparktkCoxPhModel extends TkSaveableObject {
  def train(frame: Frame,
            timeColumn: String,
            covariateColumns: List[String],
            censorColumn: String,
            convergenceTolerance: Double = 1E-6,
            maxSteps: Int = 100): CoxPhTrainReturn= {
    require(frame != null, "frame is required")
    require(timeColumn != null && timeColumn.nonEmpty, "Time column must not be null or empty")
    require(censorColumn != null && censorColumn.nonEmpty, "Censor column must not be null or empty")
    require(covariateColumns != null && covariateColumns.nonEmpty, "Co-variate columns must not be null or empty")
    require(maxSteps > 0, "Max steps must be a positive integer")

    val arguments = CoxPhTrainArgs(frame,
      timeColumn,
      covariateColumns,
      censorColumn,
      convergenceTolerance,
      maxSteps)

    // Use DataFrames to run the coxPh
    val trainFrame: DataFrame = new FrameRdd(frame.schema, frame.rdd).toCoxDataFrame(covariateColumns, timeColumn, censorColumn)
    val cox = new CoxPh()
    cox.setLabelCol("time")
    cox.setFeaturesCol("features")
    cox.setCensorCol("censor")
    cox.setMaxIter(arguments.maxSteps)
    cox.setTol(arguments.convergenceTolerance)
    val coxModel = cox.fit(trainFrame)

    new CoxPhTrainReturn(coxModel.beta.toArray.toList, coxModel.meanVector.toArray.toList)

  }

  /**
   * Load method where the work of getting the formatVersion and tkMetadata has already been done
   *
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

    SparktkCoxPhModel(sparkModel,
      coxPhMetadata.timeColumn,
      coxPhMetadata.covariateColumns,
      coxPhMetadata.censorColumn,
      coxPhMetadata.convergenceTolerance,
      coxPhMetadata.maxSteps)
  }

  /**
   * Load a CoxPhModel from the given path
   *
   * @param tc TkContext
   * @param path location
   * @return
   */
  def load(tc: TkContext, path: String): SparktkCoxPhModel = {
    tc.load(path).asInstanceOf[SparktkCoxPhModel]
  }
}
case class SparktkCoxPhModel private[survivalanalysis] (sparkModel: CoxPhModel,
                                                        timeColumn: String,
                                                        covariateColumns: List[String],
                                                        censorColumn: String,
                                                        convergenceTolerance: Double,
                                                        maxSteps: Int) extends Serializable with Model {
  implicit def rowWrapperToRowWrapperFunctions(rowWrapper: RowWrapper): RowWrapperFunctions = {
    new RowWrapperFunctions(rowWrapper)
  }

  def modelMetadata(): ModelMetaData = {
    new ModelMetaData("CoxPH Model", classOf[SparktkCoxPhModel].getName, classOf[SparkTkModelAdapter].getName, Map())
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

  override def score(row: Array[Any]): Array[Any] = ???
}

case class CoxPhMetaData(timeColumn: String,
                         covariateColumns: List[String],
                         censorColumn: String,
                         convergenceTolerance: Double,
                         maxSteps: Int) extends Serializable

case class CoxPhTrainArgs(frame: Frame,
                          timeColumn: String,
                          covariateColumns: List[String],
                          censorColumn: String,
                          convergenceTolerance: Double,
                          maxSteps: Int)

case class CoxPhTrainReturn(beta: List[Double], mean: List[Double])