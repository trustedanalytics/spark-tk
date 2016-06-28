package org.trustedanalytics.sparktk.models.clustering.gmm

import org.apache.spark.sql.Row
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.{ GaussianMixture => SparkGaussianMixture, GaussianMixtureModel => SparkGaussianMixtureModel }
import org.apache.spark.rdd.RDD
import org.trustedanalytics.sparktk.frame._
import org.trustedanalytics.sparktk.frame.internal.RowWrapper
import org.trustedanalytics.sparktk.frame.internal.rdd.{ FrameRdd, RowWrapperFunctions }
import org.trustedanalytics.sparktk.saveload.{ SaveLoad, TkSaveLoad, TkSaveableObject }
import org.json4s.JsonAST.JValue
import org.trustedanalytics.sparktk.models.MatrixImplicits._

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
   * *
   *
   * @param sc active spark context
   * @param path the source path
   * @param formatVersion the version of the format for the tk metadata that should be recorded.
   * @param tkMetadata the data to save (should be a case class), must be serializable to JSON using json4s
   * @return
   */
  def load(sc: SparkContext, path: String, formatVersion: Int, tkMetadata: JValue): Any = {

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
                                              sparkModel: SparkGaussianMixtureModel) extends Serializable {

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

}

case class GaussianMixtureModelTkMetaData(observationColumns: Seq[String],
                                          columnScalings: Seq[Double],
                                          k: Int = 2,
                                          maxIterations: Int = 100,
                                          convergenceTol: Double = 0.01,
                                          seed: Long = scala.util.Random.nextLong(),
                                          gaussians: List[List[String]]) extends Serializable