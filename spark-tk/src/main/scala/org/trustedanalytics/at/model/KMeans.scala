package org.trustedanalytics.at.model

import org.apache.spark.frame.FrameRdd
import org.apache.spark.mllib.clustering.{ KMeansModel => SparkKMeansModel }
import org.apache.spark.mllib.clustering.{ KMeans => SparkKMeans }
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.sparktk.VectorUtils
import org.trustedanalytics.at.interfaces.{ ImmutableFrame, RowWrapper, Frame }
import org.trustedanalytics.at.rdd.RowWrapperFunctions
import org.trustedanalytics.at.schema.{ Column, DataTypes }
import org.trustedanalytics.at.schema.DataTypes.DataType
import scala.collection.mutable.ListBuffer
import scala.language.implicitConversions


object KMeans {

  /**
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
            initializationMode: String = "k-means||"): KMeans = {

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

    val trainFrameRdd = new FrameRdd(frame.schema, frame.rdd)
    trainFrameRdd.cache()
    val vectorRDD = trainFrameRdd.toDenseVectorRDDWithWeights(columns, scalings)
    val model = sparkKMeans.run(vectorRDD)
    val centroids: Array[Array[Double]] = model.clusterCenters.map(_.toArray) // Make centroids easy for Python
    val sizes = KMeans.computeClusterSizes(model, trainFrameRdd, columns, scalings)
    val ssew = model.computeCost(vectorRDD)
    trainFrameRdd.unpersist()

    //Writing the kmeansModel as JSON
    //val jsonModel = new KMeansData(kmeansModel, arguments.observationColumns, arguments.columnScalings)
    //val model: Model = arguments.model
    //model.data = jsonModel.toJson.asJsObject

    KMeans(columns, scalings, k, maxIterations, epsilon, initializationMode, sizes, centroids, ssew, model)
  }

  /**
   * Computes the number of elements belonging to each cluster given the trained model and names of the frame's columns storing the observations
   * @param kmeansModel The trained KMeans Model
   * @param trainFrameRdd A Frame Rdd formed by observations used for training
   * @param observationColumns The columns of frame storing the observations
   * @param columnScalings The scalings of the observations
   * @return A map storing the cluster number and number of elements it contains
   */
  def computeClusterSizes(kmeansModel: SparkKMeansModel, trainFrameRdd: FrameRdd, observationColumns: Seq[String], columnScalings: Seq[Double]): Array[Int] = {

    val predictRDD = trainFrameRdd.mapRows(row => {
      val array = row.valuesAsArray(observationColumns).map(row => DataTypes.toDouble(row))
      val columnWeightsArray = columnScalings.toArray
      val doubles = array.zip(columnWeightsArray).map { case (x, y) => x * y }
      val point = Vectors.dense(doubles)
      kmeansModel.predict(point)
    })
    predictRDD.map(row => (row.toString, 1)).reduceByKey(_ + _).collect().map { case (k, v) => v }
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
 * @param sizes An array of length k containing the number of elements in each cluster
 * @param centroids An array of length k containing the centroid of each cluster
 * @param ssew  Within cluster sum of squared errors
 */
case class KMeans private (columns: Seq[String],
                           scalings: Seq[Double],
                           k: Int,
                           maxIterations: Int,
                           epsilon: Double,
                           initializationMode: String,
                           sizes: Array[Int],
                           centroids: Array[Array[Double]],
                           ssew: Double,
                           private val model: SparkKMeansModel) extends Serializable {

  implicit def rowWrapperToRowWrapperFunctions(rowWrapper: RowWrapper): RowWrapperFunctions = {
    new RowWrapperFunctions(rowWrapper)
  }

  /**
   *
   * @param frame - frame to add predictions to
   * @param observationColumns Column(s) containing the observations whose clusters are to be predicted.
   *                           Default is to predict the clusters over columns the KMeans model was trained on.
   *                           The columns are scaled using the same values used when training the model
   */
  def predict(frame: Frame, observationColumns: Option[Vector[String]]): Unit = {
    require(frame != null, "frame is required")
    //val frame: SparkFrame = arguments.frame
    //val model: Model = arguments.model

    //Extracting the KMeansModel from the stored JsObject
    //val kmeansData = model.data.convertTo[KMeansData]
    //val kmeansModel = kmeansData.kMeansModel
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

      for (i <- clusterCenters.indices) {
        val distance: Double = VectorUtils.toMahoutVector(point).getDistanceSquared(VectorUtils.toMahoutVector(clusterCenters(i)))
        row.addValue(distance)
      }
      val prediction = model.predict(point)
      row.addValue(prediction)
    })

    //Updating the frame schema
    var columnNames = new ListBuffer[String]()
    var columnTypes = new ListBuffer[DataTypes.DataType]()
    for (i <- model.clusterCenters.indices) {
      val colName = "distance" + i.toString
      columnNames += colName
      columnTypes += DataTypes.float64
    }
    columnNames += "predict"
    columnTypes += DataTypes.int32

    val newColumns = columnNames.toList.zip(columnTypes.toList.map(x => x: DataType))
    val updatedSchema = frame.schema.addColumns(newColumns.map { case (name, dataType) => Column(name, dataType) })

    println(s"updatedSchema=$updatedSchema")

    frame.frameState = ImmutableFrame(predictionsRDD, updatedSchema)

    // todo: change to use scala Frame AddColumnj
  }
}

