package org.trustedanalytics.at.models.kmeans

import org.apache.spark.mllib.clustering.{ KMeans => SparkKMeans, KMeansModel => SparkKMeansModel }
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.org.trustedanalytics.at.VectorUtils
import org.apache.spark.org.trustedanalytics.at.frame.FrameRdd
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.trustedanalytics.at.frame.internal.{ RowWrapper, FrameState }
import org.trustedanalytics.at.frame.internal.rdd.RowWrapperFunctions
import org.trustedanalytics.at.frame._
import DataTypes.DataType

import scala.collection.mutable.ListBuffer
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
            scalings: Seq[Double],
            k: Int = 2,
            maxIterations: Int = 20,
            epsilon: Double = 1e-4,
            seed: Option[Long] = None,
            initializationMode: String = "k-means||"): KMeansModel = {

    require(columns != null && columns.nonEmpty, "columns must not be null nor empty")
    require(scalings != null && scalings.nonEmpty, "scalings must not be null or empty")
    require(columns.length == scalings.length, "Length of columns and scalings needs to be the same")
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
    val vectorRDD = trainFrameRdd.toDenseVectorRDDWithWeights(columns, scalings)
    val model = sparkKMeans.run(vectorRDD)
    val centroids: Array[Array[Double]] = model.clusterCenters.map(_.toArray) // Make centroids easy for Python
    trainFrameRdd.unpersist()

    //Writing the kmeansModel as JSON
    //val jsonModel = new KMeansData(kmeansModel, arguments.observationColumns, arguments.columnScalings)
    //val model: Model = arguments.model
    //model.data = jsonModel.toJson.asJsObject

    KMeansModel(columns, scalings, k, maxIterations, epsilon, initializationMode, centroids, model)
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
                                        scalings: Seq[Double],
                                        k: Int,
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
   * @param frame A frame of observations used for training
   * @param observationColumns The columns of frame storing the observations (uses column names from train by default)
   * @return An array of length k of the cluster sizes
   */
  def computeClusterSizes(frame: Frame, observationColumns: Option[Seq[String]] = None): Array[Int] = {
    val frameRdd = new FrameRdd(frame.schema, frame.rdd)
    val obs = observationColumns.getOrElse(columns)
    val predictRDD = frameRdd.mapRows(row => {
      val array = row.valuesAsArray(obs).map(row => DataTypes.toDouble(row))
      val columnWeightsArray = scalings.toArray
      val doubles = array.zip(columnWeightsArray).map { case (x, y) => x * y }
      val point = Vectors.dense(doubles)
      model.predict(point)
    })
    val clusterSizes = predictRDD.map(row => (row.toString, 1)).reduceByKey(_ + _).collect().map { case (k, v) => v }
    clusterSizes
  }

  /**
   * Computes the 'within cluster sum of squared errors' for the given frame
   */
  def computeCost(frame: Frame, observationColumns: Option[Vector[String]] = None): Double = {
    val frameRdd = new FrameRdd(frame.schema, frame.rdd)
    val vectorRDD = frameRdd.toDenseVectorRDDWithWeights(columns, scalings)
    model.computeCost(vectorRDD)
  }

  /**
   *
   * @param frame
   * @param observationColumns
   */
  def addDistanceColumns(frame: Frame, observationColumns: Option[Vector[String]] = None): Unit = {
    val kmeansColumns = observationColumns.getOrElse(columns)
    val frameRdd = new FrameRdd(frame.schema, frame.rdd)
    val predictionsRDD: RDD[Row] = frameRdd.mapRows(row => {
      val columnsArray = row.valuesAsDenseVector(kmeansColumns).toArray
      val columnScalingsArray = scalings.toArray
      val doubles = columnsArray.zip(columnScalingsArray).map { case (x, y) => x * y }
      val point = Vectors.dense(doubles)

      val clusterCenters = model.clusterCenters

      for (i <- clusterCenters.indices) {
        val distance: Double = VectorUtils.toMahoutVector(point).getDistanceSquared(VectorUtils.toMahoutVector(clusterCenters(i)))
        row.addValue(distance)
      }
      row.row
    })
    var columnNames = new ListBuffer[String]()
    var columnTypes = new ListBuffer[DataTypes.DataType]()
    for (i <- model.clusterCenters.indices) {
      val colName = "distance" + i.toString
      columnNames += colName
      columnTypes += DataTypes.float64
    }
    val newColumns = columnNames.toList.zip(columnTypes.toList.map(x => x: DataType))
    val updatedSchema = frame.schema.addColumns(newColumns.map { case (name, dataType) => Column(name, dataType) })

    frame.frameState = FrameState(predictionsRDD, updatedSchema)
  }

  /**
   *
   * @param frame - frame to add predictions to
   * @param observationColumns Column(s) containing the observations whose clusters are to be predicted.
   *                           Default is to predict the clusters over columns the KMeans model was trained on.
   *                           The columns are scaled using the same values used when training the model
   */
  def predict(frame: Frame, observationColumns: Option[Vector[String]] = None): Unit = {
    require(frame != null, "frame is required")

    if (observationColumns.isDefined) {
      require(scalings.length == observationColumns.get.length, "Number of columns for train and predict should be same")
    }

    val kmeansColumns = observationColumns.getOrElse(columns)
    val scalingValues = scalings

    val frameRdd = new FrameRdd(frame.schema, frame.rdd)
    val predictionsRDD = frameRdd.mapRows(row => {
      val columnsArray = row.valuesAsDenseVector(kmeansColumns).toArray
      val columnScalingsArray = scalingValues.toArray
      val doubles = columnsArray.zip(columnScalingsArray).map { case (x, y) => x * y }
      val point = Vectors.dense(doubles)

      val clusterCenters = model.clusterCenters

      //      for (i <- clusterCenters.indices) {
      //        val distance: Double = VectorUtils.toMahoutVector(point).getDistanceSquared(VectorUtils.toMahoutVector(clusterCenters(i)))
      //        row.addValue(distance)
      //      }
      val prediction = model.predict(point)
      row.addValue(prediction)
    })

    //Updating the frame schema
    var columnNames = new ListBuffer[String]()
    var columnTypes = new ListBuffer[DataTypes.DataType]()
    columnNames += "cluster"
    columnTypes += DataTypes.int32

    val newColumns = columnNames.toList.zip(columnTypes.toList.map(x => x: DataType))
    val updatedSchema = frame.schema.addColumns(newColumns.map { case (name, dataType) => Column(name, dataType) })

    frame.frameState = FrameState(predictionsRDD, updatedSchema)

    // todo: change to use scala Frame AddColumns
  }
}

