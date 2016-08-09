package org.trustedanalytics.sparktk.models.clustering.kmeans

import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.{ KMeans => SparkKMeans, KMeansModel => SparkKMeansModel }
import org.apache.spark.mllib.linalg.{ Vector => MllibVector }
import org.apache.spark.sql.Row
import org.trustedanalytics.sparktk.TkContext
import org.trustedanalytics.sparktk.frame.internal.RowWrapper
import org.trustedanalytics.sparktk.frame.internal.rdd.{ VectorUtils, FrameRdd, RowWrapperFunctions }
import org.trustedanalytics.sparktk.frame._
import org.trustedanalytics.sparktk.saveload.{ SaveLoad, TkSaveLoad, TkSaveableObject }

import scala.language.implicitConversions
import org.json4s.JsonAST.JValue

object KMeansModel extends TkSaveableObject {

  /**
   * @param frame The frame containing the data to train on
   * @param columns The columns to train on
   * @param k Desired number of clusters
   * @param scalings Optional scaling values, each is multiplied by the corresponding value in the observation column
   * @param maxIterations Number of iterations for which the algorithm should run
   * @param epsilon Distance threshold within which we consider k-means to have converged. Default is 1e-4. If all
   *                centers move less than this Euclidean distance, we stop iterating one run
   * @param initializationMode The initialization technique for the algorithm.  It could be either "random" to
   *                           choose random points as initial clusters, or "k-means||" to use a parallel variant
   *                           of k-means++.  Default is "k-means||"
   * @param seed Optional seed value to control the algorithm randomness
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
      case Some(weights) => trainFrameRdd.toDenseVectorRddWithWeights(columns, weights)
      case None => trainFrameRdd.toDenseVectorRdd(columns)
    }
    val model = sparkKMeans.run(vectorRDD)
    trainFrameRdd.unpersist()

    KMeansModel(columns, k, scalings, maxIterations, epsilon, initializationMode, seed, model)
  }

  /**
   * Load a KMeansModel from the given path
   * @param tc TkContext
   * @param path location
   * @return
   */
  def load(tc: TkContext, path: String): KMeansModel = {
    tc.load(path).asInstanceOf[KMeansModel]
  }

  def loadTkSaveableObject(sc: SparkContext, path: String, formatVersion: Int, tkMetadata: JValue): Any = {

    validateFormatVersion(formatVersion, 1)
    val m: KMeansModelTkMetaData = SaveLoad.extractFromJValue[KMeansModelTkMetaData](tkMetadata)
    val sparkModel = SparkKMeansModel.load(sc, path)

    KMeansModel(m.columns, m.k, m.scalings, m.maxIterations, m.epsilon, m.initializationMode, m.seed, sparkModel)
  }

  /**
   * Helper method that provides a function to create an appropriate MllibVector for a RowWrapper, taking into account optional weights
   * @param observationColumns The columns which hold the observations
   * @return
   */
  private[kmeans] def getDenseVectorMaker(observationColumns: Seq[String], scalings: Option[Seq[Double]]): RowWrapper => MllibVector = {

    def getDenseVector(columnNames: Seq[String])(row: RowWrapper): MllibVector = {
      row.toDenseVector(columnNames)
    }

    def getWeightedDenseVector(columnNames: Seq[String], columnWeights: Array[Double])(row: RowWrapper): MllibVector = {
      row.toWeightedDenseVector(columnNames, columnWeights)
    }

    scalings match {
      case None => getDenseVector(observationColumns)
      case Some(weights) =>
        require(weights.length == observationColumns.length)
        getWeightedDenseVector(observationColumns, weights.toArray)
    }
  }
}

/**
 * KMeansModel
 * @param columns The names of the columns trained on
 * @param k Desired number of clusters
 * @param scalings Optional scaling values, each is multiplied by the corresponding value in the observation column
 * @param maxIterations Number of iterations for which the algorithm should run
 * @param epsilon Distance threshold within which we consider k-means to have converged. Default is 1e-4. If all
 *                centers move less than this Euclidean distance, we stop iterating one run
 * @param initializationMode The initialization technique for the algorithm.  It could be either "random" to
 *                           choose random points as initial clusters, or "k-means||" to use a parallel variant
 *                           of k-means++.  Default is "k-means||"
 * @param seed Optional seed value to control the algorithm randomness
 * @param sparkModel SparkModel created from the training process
 */
case class KMeansModel private[kmeans] (columns: Seq[String],
                                        k: Int,
                                        scalings: Option[Seq[Double]],
                                        maxIterations: Int,
                                        epsilon: Double,
                                        initializationMode: String,
                                        seed: Option[Long] = None,
                                        sparkModel: SparkKMeansModel) extends Serializable {

  implicit def rowWrapperToRowWrapperFunctions(rowWrapper: RowWrapper): RowWrapperFunctions = {
    new RowWrapperFunctions(rowWrapper)
  }

  def centroids: Array[MllibVector] = sparkModel.clusterCenters

  def centroidsAsArrays: Array[Array[Double]] = sparkModel.clusterCenters.map(_.toArray) // Make centroids easy for Python

  /**
   * Computes the number of elements belonging to each cluster given the trained model and names of the frame's columns storing the observations
   * @param frame A frame containing observations
   * @param observationColumns The columns of frame storing the observations (uses column names from train by default)
   * @return An array of length k of the cluster sizes
   */
  def computeClusterSizes(frame: Frame, observationColumns: Option[Seq[String]] = None): Array[Int] = {
    require(frame != null, "frame is required")
    require(observationColumns != null, "observationColumns cannot be null (can be None)")
    if (observationColumns.isDefined) {
      require(columns.length == observationColumns.get.length, "Number of columns for train and predict should be same")
    }
    val vectorMaker = KMeansModel.getDenseVectorMaker(observationColumns.getOrElse(columns), scalings)
    val frameRdd = new FrameRdd(frame.schema, frame.rdd)
    val predictRDD = frameRdd.mapRows(row => {
      val point = vectorMaker(row)
      sparkModel.predict(point)
    })
    val clusterSizes = predictRDD.map(row => (row.toString, 1)).reduceByKey(_ + _).collect().map { case (_, v) => v }
    clusterSizes
  }

  /**
   * Computes the 'within cluster sum of squared errors' for the given frame
   * @param frame A frame containing observations
   * @param observationColumns columns of the frame storing the observations (uses column names from train by default)
   * @return wsse
   */
  def computeWsse(frame: Frame, observationColumns: Option[Vector[String]] = None): Double = {
    require(frame != null, "frame is required")
    require(observationColumns != null, "observationColumns cannot be null (can be None)")
    if (observationColumns.isDefined) {
      require(columns.length == observationColumns.get.length, "Number of columns for train and predict should be same")
    }

    val frameRdd = new FrameRdd(frame.schema, frame.rdd)
    val vectorRdd = frameRdd.toDenseVectorRdd(observationColumns.getOrElse(columns), scalings)
    sparkModel.computeCost(vectorRdd)
  }

  /**
   * Computes the distances to each centroid adds each one as a new column to the given frame
   * @param frame A frame containing observations
   * @param observationColumns columns of the frame storing the observations (uses column names from train by default)
   */
  def addDistanceColumns(frame: Frame, observationColumns: Option[Vector[String]] = None): Unit = {
    require(frame != null, "frame is required")
    require(observationColumns != null, "observationColumns cannot be null (can be None)")
    if (observationColumns.isDefined) {
      require(columns.length == observationColumns.get.length, "Number of columns for train and predict should be same")
    }

    val vectorMaker = KMeansModel.getDenseVectorMaker(observationColumns.getOrElse(columns), scalings)
    val distanceMapper: RowWrapper => Row = row => {
      val point = vectorMaker(row)
      val clusterCenters = sparkModel.clusterCenters
      Row.fromSeq(for (i <- clusterCenters.indices) yield {
        val distance: Double = VectorUtils.toMahoutVector(point).getDistanceSquared(VectorUtils.toMahoutVector(clusterCenters(i)))
        distance
      })
    }

    val newColumns = (for (i <- sparkModel.clusterCenters.indices) yield Column("distance" + i.toString, DataTypes.float64)).toSeq
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
      require(columns.length == observationColumns.get.length, s"Number of columns for train and predict should be same (train columns=$columns, observation columns=$observationColumns)")
    }

    val vectorMaker = KMeansModel.getDenseVectorMaker(observationColumns.getOrElse(columns), scalings)
    val predictMapper: RowWrapper => Row = row => {
      val point = vectorMaker(row)
      val prediction = sparkModel.predict(point)
      Row.apply(prediction)
    }

    frame.addColumns(predictMapper, Seq(Column(frame.schema.getNewColumnName("cluster"), DataTypes.int32)))
  }

  /**
   * Saves this model to a file
   * @param sc active SparkContext
   * @param path save to path
   */
  def save(sc: SparkContext, path: String): Unit = {
    sparkModel.save(sc, path)
    val formatVersion: Int = 1
    val tkMetadata = KMeansModelTkMetaData(columns, k, scalings, maxIterations, epsilon, initializationMode, seed)
    TkSaveLoad.saveTk(sc, path, KMeansModel.formatId, formatVersion, tkMetadata)
  }
}

/**
 * TK Metadata that will be stored as part of the model
 * @param columns The names of the columns trained on
 * @param k Desired number of clusters
 * @param scalings Optional scaling values, each is multiplied by the corresponding value in the observation column
 * @param maxIterations Number of iterations for which the algorithm should run
 * @param epsilon Distance threshold within which we consider k-means to have converged. Default is 1e-4. If all
 *                centers move less than this Euclidean distance, we stop iterating one run
 * @param initializationMode The initialization technique for the algorithm.  It could be either "random" to
 *                           choose random points as initial clusters, or "k-means||" to use a parallel variant
 *                           of k-means++.  Default is "k-means||"
 * @param seed Optional seed value to control the algorithm randomness
 */
case class KMeansModelTkMetaData(columns: Seq[String],
                                 k: Int,
                                 scalings: Option[Seq[Double]],
                                 maxIterations: Int,
                                 epsilon: Double,
                                 initializationMode: String,
                                 seed: Option[Long] = None) extends Serializable

// todo: blbarker - create macros to enable us to compose case classes, such that we can remove this boilerplate copy going on with
// train args, the model, the model tkmetadata, in addition to the scaladoc redundancy (not to mention python!)
