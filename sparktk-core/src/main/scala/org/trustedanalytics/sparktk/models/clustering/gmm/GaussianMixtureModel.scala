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
package org.trustedanalytics.sparktk.models.clustering.gmm

import java.nio.file.{ Files, Path }

import org.apache.spark.sql.Row
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.{ GaussianMixture => SparkGaussianMixture, GaussianMixtureModel => SparkGaussianMixtureModel }
import org.apache.spark.rdd.RDD
import org.trustedanalytics.sparktk.TkContext
import org.trustedanalytics.sparktk.frame._
import org.trustedanalytics.sparktk.frame.internal.RowWrapper
import org.trustedanalytics.sparktk.frame.internal.rdd.{ FrameRdd, RowWrapperFunctions }
import org.trustedanalytics.sparktk.saveload.{ SaveLoad, TkSaveLoad, TkSaveableObject }
import org.json4s.JsonAST.JValue
import org.trustedanalytics.sparktk.models.MatrixImplicits._
import java.io.{ FileOutputStream, File }
import org.apache.spark.mllib.linalg.Vectors
import org.apache.commons.io.{ IOUtils, FileUtils }
import org.trustedanalytics.sparktk.models.{ SparkTkModelAdapter, TkSearchPath, ScoringModelUtils }
import org.trustedanalytics.scoring.interfaces.{ ModelMetaDataArgs, Field, Model }

object GaussianMixtureModel extends TkSaveableObject {

  /**
   *
   * @param frame A frame to train the model on
   * @param observationColumns Columns containing the observations
   * @param columnScalings Column scalings for each of the observation columns.
   *                       The scaling value is multiplied by the corresponding value in the observation column
   * @param k Desired number of clusters. Default is 2
   * @param maxIterations Number of iterations for which the algorithm should run. Default is 100.
   * @param convergenceTol Largest change in log-likelihood at which convergence is considered to have occurred.
   * @param seed Random seed
   * @return GaussianMixtureModel The trained model
   */
  def train(frame: Frame,
            observationColumns: Seq[String],
            columnScalings: Seq[Double],
            k: Int = 2,
            maxIterations: Int = 100,
            convergenceTol: Double = 0.01,
            seed: Long = scala.util.Random.nextLong()): GaussianMixtureModel = {

    require(frame != null, "frame must not be null")
    require(observationColumns != null && observationColumns.nonEmpty, "observationColumn must not be null nor empty")
    require(columnScalings != null && columnScalings.nonEmpty, "columnWeights must not be null or empty")
    require(columnScalings.length == observationColumns.length, "Length of columnWeights and observationColumns needs to be the same")
    require(k > 0, "k must be at least 1")
    require(maxIterations > 0, "maxIterations must be a positive value")

    val sparkGaussianMixture = new SparkGaussianMixture()
    sparkGaussianMixture.setK(k)
    sparkGaussianMixture.setConvergenceTol(convergenceTol)
    sparkGaussianMixture.setMaxIterations(maxIterations)
    sparkGaussianMixture.setSeed(seed)

    val trainFrameRdd = new FrameRdd(frame.schema, frame.rdd)
    trainFrameRdd.cache()
    val vectorRDD = trainFrameRdd.toDenseVectorRddWithWeights(observationColumns, columnScalings)
    val model = sparkGaussianMixture.run(vectorRDD)
    trainFrameRdd.unpersist()

    val gaussians = model.gaussians.map(i => List("mu:" + i.mu.toString, "sigma:" + i.sigma.toListOfList())).toList

    GaussianMixtureModel(observationColumns,
      columnScalings,
      k,
      maxIterations,
      convergenceTol,
      seed,
      gaussians,
      model)
  }

  /**
   * Load a GaussianMixtureModel from the given path
   * @param tc TkContext
   * @param path location
   * @return
   */
  def load(tc: TkContext, path: String): GaussianMixtureModel = {
    tc.load(path).asInstanceOf[GaussianMixtureModel]
  }

  /**
   * *
   *
   * @param sc active spark context
   * @param path the source path
   * @param formatVersion the version of the format for the tk metadata that should be recorded.
   * @param tkMetadata the data to save (should be a case class), must be serializable to JSON using json4s
   * @return
   */
  def loadTkSaveableObject(sc: SparkContext, path: String, formatVersion: Int, tkMetadata: JValue): Any = {

    validateFormatVersion(formatVersion, 1)
    val m: GaussianMixtureModelTkMetaData = SaveLoad.extractFromJValue[GaussianMixtureModelTkMetaData](tkMetadata)
    val sparkModel = SparkGaussianMixtureModel.load(sc, path)

    GaussianMixtureModel(m.observationColumns, m.columnScalings, m.k, m.maxIterations, m.convergenceTol, m.seed, m.gaussians, sparkModel)
  }

}

case class GaussianMixtureModel private[gmm] (observationColumns: Seq[String],
                                              columnScalings: Seq[Double],
                                              k: Int = 2,
                                              maxIterations: Int = 100,
                                              convergenceTol: Double = 0.01,
                                              seed: Long = scala.util.Random.nextLong(),
                                              gaussians: List[List[String]],
                                              sparkModel: SparkGaussianMixtureModel) extends Serializable with Model {

  implicit def rowWrapperToRowWrapperFunctions(rowWrapper: RowWrapper): RowWrapperFunctions = {
    new RowWrapperFunctions(rowWrapper)
  }

  /**
   *
   * @param frame The frame to compute the cluster size on, after prediction
   * @return Returns a map of cluster names(string) and sizes(integer)
   */
  def computeGmmClusterSize(frame: Frame): Map[String, Int] = {
    val trainFrameRdd = new FrameRdd(frame.schema, frame.rdd)
    val vectorRDD = trainFrameRdd.toDenseVectorRddWithWeights(observationColumns, columnScalings)
    val predictRDD = sparkModel.predict(vectorRDD)
    predictRDD.map(row => ("Cluster:" + row.toString, 1)).reduceByKey(_ + _).collect().toMap
  }

  /**
   * @param frame frame whose cluster assignments are to be predicted
   * @param observationColumns Column(s) containing the observations whose clusters are to be predicted. By default,
   *                           we predict the clusters over columns the GMMModel was trained on. The columns are
   *                           scaled using the same values used when training the model
   */
  def predict(frame: Frame, observationColumns: Option[Seq[String]] = None): Unit = {
    require(frame != null, "frame is required")
    if (observationColumns.isDefined) {
      require(observationColumns.get.length == observationColumns.get.length, "Number of columns for train and predict should be same")
    }

    val gmmColumns = observationColumns.getOrElse(this.observationColumns)
    val scalingValues = columnScalings
    val frameRdd = new FrameRdd(frame.schema, frame.rdd)

    val predictionsRdd = sparkModel.predict(frameRdd.toDenseVectorRdd(gmmColumns, Some(scalingValues)))
    val indexedPredictionsRdd = predictionsRdd.zipWithIndex().map { case (cluster, index) => (index.toLong, Row.apply(cluster)) }

    val indexedFrameRdd = frame.rdd.zipWithIndex().map { case (row, index) => (index, row) }

    val resultRdd: RDD[Row] = indexedPredictionsRdd.join(indexedFrameRdd).map { value =>
      val row = value._2._2
      val cluster = value._2._1
      Row.merge(row, cluster)
    }
    frame.init(resultRdd, frame.schema.copy(columns = frame.schema.columns ++ Seq(Column("predicted_cluster", DataTypes.int32))))
  }

  /**
   * Saves this model to a file
   *
   * @param sc active SparkContext
   * @param path save to path
   */
  def save(sc: SparkContext, path: String): Unit = {
    sparkModel.save(sc, path)
    val formatVersion: Int = 1
    val tkMetadata = GaussianMixtureModelTkMetaData(observationColumns, columnScalings, k, maxIterations, convergenceTol, seed, gaussians)
    TkSaveLoad.saveTk(sc, path, GaussianMixtureModel.formatId, formatVersion, tkMetadata)
  }

  /**
   *
   * @param row
   * @return
   */
  def score(row: Array[Any]): Array[Any] = {
    //    val x: Array[Double] = new Array[Double](row.length)
    //    row.zipWithIndex.foreach {
    //      case (value: Any, index: Int) => x(index) = ScoringModelUtils.asDouble(value)
    //    }
    //    row :+ sparkModel.predict(Vectors.dense(x))
    throw new NotImplementedError()
  }

  /**
   *
   * @return fields containing the input names and their datatypes
   */
  def input(): Array[Field] = {
    var input = Array[Field]()
    observationColumns.foreach { name =>
      input = input :+ Field(name, "Double")
    }
    input
  }

  /**
   *
   * @return fields containing the input names and their datatypes along with the output and its datatype
   */
  def output(): Array[Field] = {
    val output = input()
    output :+ Field("Score", "Int")
  }

  /**
   *
   * @return
   */
  def modelMetadata(map: Map[String, String]): ModelMetaDataArgs = {
    new ModelMetaDataArgs("Gaussian Mixture Model", classOf[GaussianMixtureModel].getName, classOf[SparkTkModelAdapter].getName, map)
  }

  /**
   *
   * @return
   */
  def modelMetadata(): ModelMetaDataArgs = {
    new ModelMetaDataArgs("Gaussian Mixture Model", classOf[GaussianMixtureModel].getName, classOf[SparkTkModelAdapter].getName, Map())
  }
  /**
   *
   * @param marSavePath
   * @return
   */
  def exportToMar(sc: SparkContext, marSavePath: String): Unit = {
    var tmpDir: Path = null
    try {
      tmpDir = Files.createTempDirectory("sparktk-scoring-model")
      save(sc, "file://" + tmpDir.toString)
      ScoringModelUtils.saveToMar(marSavePath, classOf[GaussianMixtureModel].getName, tmpDir)
    }
    finally {
      sys.addShutdownHook(FileUtils.deleteQuietly(tmpDir.toFile)) // Delete temporary directory on exit
    }
  }

}

case class GaussianMixtureModelTkMetaData(observationColumns: Seq[String],
                                          columnScalings: Seq[Double],
                                          k: Int,
                                          maxIterations: Int,
                                          convergenceTol: Double,
                                          seed: Long,
                                          gaussians: List[List[String]]) extends Serializable