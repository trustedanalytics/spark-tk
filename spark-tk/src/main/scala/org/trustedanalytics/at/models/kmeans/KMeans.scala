package org.trustedanalytics.at.models.kmeans

import org.apache.spark.mllib.clustering.{ KMeans => SparkKMeans, KMeansModel => SparkKMeansModel }
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.{ Vector => LinalgVector }
import org.apache.spark.org.trustedanalytics.at.VectorUtils
import org.apache.spark.org.trustedanalytics.at.frame.FrameRdd
import org.apache.spark.sql.Row
import org.trustedanalytics.at.frame.internal.RowWrapper
import org.trustedanalytics.at.frame.internal.rdd.RowWrapperFunctions
import org.trustedanalytics.at.frame._

import scala.language.implicitConversions

object KMeans {

  /**
   * @param frame The frame containing the data to train on
   * @param columns The columns to train on
   * @param scalings The scaling value is multiplied by the corresponding value in the observation column
   * @param k Desired number of clusters
   * @param maxIterations Number of iteration for which the algorithm should run
   * @param epsilon Distance threshold within which we consider k-means to have converged. Default is 1e-4. If all
   *                centers move less than this Euclidean distance, we stop iterating one run
   * @param initializationMode The initialization technique for the algorithm.  It could be either "random" to
   *                           choose random points as initial clusters, or "k-means||" to use a parallel variant
   *                           of k-means++.  Default is "k-means||"
   */
  def train(frame: Frame,
            columns: Seq[String],
            k: Int = 2,
            scalings: Option[Seq[Double]] = None,
            maxIterations: Int = 20,
            epsilon: Double = 1e-4,
            initializationMode: String = "k-means||",
            seed: Option[Long] = None): KMeansModel = {

    require(columns != null && columns.nonEmpty, "columns must not be null nor empty")
    require(scalings != null, "scalings must not be null")
    if (scalings.isDefined) {
      require(columns.length == scalings.get.length, "Length of columns and scalings needs to be the same")
    }
    require(k > 0, "k must be at least 1")
    require(maxIterations > 0, "maxIterations must be a positive value")
    require(epsilon > 0.0, "epsilon must be a positive value")
    require(initializationMode == "random" || initializationMode == "k-means||", "initialization mode must be 'random' or 'k-means||'")

    val sparkKMeans = new SparkKMeans()
    sparkKMeans.setK(k)
    sparkKMeans.setMaxIterations(maxIterations)
    sparkKMeans.setInitializationMode(initializationMode)
    sparkKMeans.setEpsilon(epsilon)
    if (seed.isDefined) {
      sparkKMeans.setSeed(seed.get)
    }

    val trainFrameRdd = new FrameRdd(frame.schema, frame.rdd)
    trainFrameRdd.cache()
    val vectorRDD = scalings match {
      case Some(weights) => trainFrameRdd.toDenseVectorRDDWithWeights(columns, weights)
      case None => trainFrameRdd.toDenseVectorRDD(columns)
    }
    val model = sparkKMeans.run(vectorRDD)
    val centroids: Array[Array[Double]] = model.clusterCenters.map(_.toArray) // Make centroids easy for Python
    trainFrameRdd.unpersist()

    //Writing the kmeansModel as JSON
    //val jsonModel = new KMeansData(kmeansModel, arguments.observationColumns, arguments.columnScalings)
    //val model: Model = arguments.model
    //model.data = jsonModel.toJson.asJsObject

    KMeansModel(columns, k, scalings, maxIterations, epsilon, initializationMode, centroids, model)
  }

  def getDenseVectorRdd(frameRdd: FrameRdd, columns: Seq[String], scalings: Option[Seq[Double]]) = {
    scalings match {
      case Some(weights) => frameRdd.toDenseVectorRDDWithWeights(columns, weights)
      case None => frameRdd.toDenseVectorRDD(columns)
    }
  }
}

/**
 * @param scalings The scaling value is multiplied by the corresponding value in the observation column
 * @param k Desired number of clusters
 * @param maxIterations Number of iteration for which the algorithm should run
 * @param epsilon Distance threshold within which we consider k-means to have converged. Default is 1e-4. If all
 *                centers move less than this Euclidean distance, we stop iterating one run
 * @param initializationMode The initialization technique for the algorithm.  It could be either "random" to
 *                           choose random points as initial clusters, or "k-means||" to use a parallel variant
 *                           of k-means++.  Default is "k-means||"
 * @param centroids An array of length k containing the centroid of each cluster
 */
case class KMeansModel private[kmeans] (columns: Seq[String],
                                        k: Int,
                                        scalings: Option[Seq[Double]],
                                        maxIterations: Int,
                                        epsilon: Double,
                                        initializationMode: String,
                                        centroids: Array[Array[Double]],
                                        private val model: SparkKMeansModel) extends Serializable {

  implicit def rowWrapperToRowWrapperFunctions(rowWrapper: RowWrapper): RowWrapperFunctions = {
    new RowWrapperFunctions(rowWrapper)
  }

  /**
   * Computes the number of elements belonging to each cluster given the trained model and names of the frame's columns storing the observations
   * @param frame A frame containing observations
   * @param observationColumns The columns of frame storing the observations (uses column names from train by default)
   * @return An array of length k of the cluster sizes
   */
  def computeClusterSizes(frame: Frame, observationColumns: Option[Seq[String]] = None): Array[Int] = {
    require(frame != null, "frame is required")
    if (observationColumns.isDefined) {
      require(columns.length == observationColumns.get.length, "Number of columns for train and predict should be same")
    }

    val vectorMaker = getDenseVectorMaker(observationColumns.getOrElse(columns), scalings)
    val frameRdd = new FrameRdd(frame.schema, frame.rdd)
    val predictRDD = frameRdd.mapRows(row => {
      val point = vectorMaker(row)
      model.predict(point)
    })
    val clusterSizes = predictRDD.map(row => (row.toString, 1)).reduceByKey(_ + _).collect().map { case (k, v) => v }
    clusterSizes
  }

  // todo - move to RowWrapper
  def getRowValuesAsDoubles(featureColumnNames: Seq[String])(row: RowWrapper): Array[Double] = {
    row.valuesAsArray(featureColumnNames).map(row => DataTypes.toDouble(row))
  }

  // todo - move to a RowWrapper (or DenseVector lib)
  /**
   * Returns a function which will create an appropriate LinalgVector for a RowWrapper, taking into account
   * optional weights
   * @param featureColumnNames
   * @param columnWeights
   * @return
   */
  def getDenseVectorMaker(featureColumnNames: Seq[String], columnWeights: Option[Seq[Double]]): RowWrapper => LinalgVector = {

    def getDenseVector(featureColumnNames: Seq[String])(row: RowWrapper): LinalgVector = {
      Vectors.dense(getRowValuesAsDoubles(featureColumnNames)(row))
    }

    def getWeightedDenseVector(featureColumnNames: Seq[String], columnWeights: Array[Double])(row: RowWrapper): LinalgVector = {
      val values = getRowValuesAsDoubles(featureColumnNames)(row)
      val scaledValues = values.zip(columnWeights).map { case (x, y) => x * y }
      Vectors.dense(scaledValues)
    }

    scalings match {
      case None => getDenseVector(featureColumnNames)
      case Some(weights) =>
        require(weights.length == featureColumnNames.length)
        getWeightedDenseVector(featureColumnNames, weights.toArray)
    }
  }

  /**
   * Computes the 'within cluster sum of squared errors' for the given frame
   * @param frame A frame containing observations
   * @param observationColumns The columns of frame storing the observations (uses column names from train by default)
   * @return wsse
   */
  def computeWsse(frame: Frame, observationColumns: Option[Vector[String]] = None): Double = {
    val frameRdd = new FrameRdd(frame.schema, frame.rdd)
    val vectorRdd = KMeans.getDenseVectorRdd(frameRdd, observationColumns.getOrElse(columns), scalings)
    model.computeCost(vectorRdd)
  }

  def addDistanceColumns(frame: Frame, observationColumns: Option[Vector[String]] = None): Unit = {
    require(frame != null, "frame is required")
    if (observationColumns.isDefined) {
      require(columns.length == observationColumns.get.length, "Number of columns for train and predict should be same")
    }

    val vectorMaker = getDenseVectorMaker(observationColumns.getOrElse(columns), scalings)
    val distanceMapper: RowWrapper => Row = row => {
      val point = vectorMaker(row)
      val clusterCenters = model.clusterCenters
      Row.fromSeq(for (i <- clusterCenters.indices) yield {
        val distance: Double = VectorUtils.toMahoutVector(point).getDistanceSquared(VectorUtils.toMahoutVector(clusterCenters(i)))
        distance
      })
    }

    val newColumns = (for (i <- model.clusterCenters.indices) yield Column("distance" + i.toString, DataTypes.float64)).toSeq
    frame.addColumns(distanceMapper, newColumns)
  }

  /**
   * Adds a column to the frame which indicates the predicted cluster for each observation
   * @param frame - frame to add predictions to
   * @param observationColumns Column(s) containing the observations whose clusters are to be predicted.
   *                           Default is to predict the clusters over columns the KMeans model was trained on.
   *                           The columns are scaled using the same values used when training the model
   */
  def predict(frame: Frame, observationColumns: Option[Vector[String]] = None): Unit = {
    require(frame != null, "frame is required")
    if (observationColumns.isDefined) {
      require(columns.length == observationColumns.get.length, "Number of columns for train and predict should be same")
    }

    val vectorMaker = getDenseVectorMaker(observationColumns.getOrElse(columns), scalings)
    val scalingArray = scalings.toArray
    val predictMapper: RowWrapper => Row = row => {
      val point = vectorMaker(row)
      val prediction = model.predict(point)
      Row.apply(prediction)
    }

    frame.addColumns(predictMapper, Seq(Column("cluster", DataTypes.int32)))
  }
}
