package org.trustedanalytics.sparktk.models.collaborativefiltering

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{ Vector => MllibVector }
import org.apache.spark.mllib.recommendation.{ MatrixFactorizationModel, ALS, Rating }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.json4s.JsonAST.JValue
import org.trustedanalytics.sparktk.frame.internal.RowWrapper
import org.trustedanalytics.sparktk.frame.internal.rdd.{ VectorUtils, FrameRdd, RowWrapperFunctions }
import org.trustedanalytics.sparktk.frame._
import org.trustedanalytics.sparktk.saveload.{ SaveLoad, TkSaveLoad, TkSaveableObject }

object CollaborativeFilteringModel extends TkSaveableObject {

  /**
   *
   * @param frame The frame containing the data to train on
   * @param sourceColumnName source column name.
   * @param destColumnName destination column name.
   * @param weightColumnName weight column name.
   * @param maxSteps max number of super-steps (max iterations) before the algorithm terminates. Default = 10
   * @param regularization float value between 0 .. 1
   * @param alpha double value between 0 .. 1
   * @param numFactors number of the desired factors (rank)
   * @param useImplicit use implicit preference
   * @param numUserBlocks number of user blocks
   * @param numItemBlock number of item blocks
   * @param checkpointIterations Number of iterations between checkpoints
   * @param targetRMSE target RMSE
   * @return CollaborativeFilteringModel
   */
  def train(frame: Frame,
            sourceColumnName: String,
            destColumnName: String,
            weightColumnName: String,
            maxSteps: Int = 10,
            regularization: Float = 0.5f,
            alpha: Double = 0.5f,
            numFactors: Int = 3,
            useImplicit: Boolean = false,
            numUserBlocks: Int = 2,
            numItemBlock: Int = 3,
            checkpointIterations: Int = 10,
            targetRMSE: Double = 0.05): CollaborativeFilteringModel = {

    require(frame != null, "input frame is required")
    require(StringUtils.isNotEmpty(sourceColumnName), "source column name is required")
    require(StringUtils.isNotEmpty(destColumnName), "destination column name is required")
    require(StringUtils.isNotEmpty(weightColumnName), "weight column name is required")
    require(maxSteps > 1, "min steps must be a positive integer")
    require(regularization > 0, "regularization must be a positive value")
    require(alpha > 0, "alpha must be a positive value")
    require(checkpointIterations > 0, "Iterations between checkpoints must be positive")
    require(numFactors > 0, "number of factors must be a positive integer")
    require(numUserBlocks > 0, "number of user blocks must be a positive integer")
    require(numItemBlock > 0, "number of item blocks must be a positive integer")
    require(targetRMSE > 0, "target RMSE must be a positive value")

    val schema = frame.schema
    val frameRdd = new FrameRdd(frame.schema, frame.rdd)

    schema.requireColumnIsType(sourceColumnName, DataTypes.int)
    schema.requireColumnIsType(destColumnName, DataTypes.int)

    val alsInput = frameRdd.mapRows(row => {
      Rating(row.intValue(sourceColumnName),
        row.intValue(destColumnName),
        row.doubleValue(weightColumnName))
    })

    val als = new ALS()
      .setCheckpointInterval(checkpointIterations)
      .setRank(numFactors)
      .setIterations(maxSteps)
      .setRank(numFactors)
      .setLambda(regularization)
      .setAlpha(alpha)

    val alsTrainedModel: MatrixFactorizationModel = als.run(alsInput)

    CollaborativeFilteringModel(sourceColumnName,
      destColumnName,
      weightColumnName,
      maxSteps,
      regularization,
      alpha,
      numFactors,
      useImplicit,
      numUserBlocks,
      numItemBlock,
      checkpointIterations,
      targetRMSE,
      alsTrainedModel.rank,
      alsTrainedModel)
  }

  def load(sc: SparkContext, path: String, formatVersion: Int, tkMetaData: JValue): Any = {
    validateFormatVersion(formatVersion, 1)
    val m: CollaborativeFilteringMetaData = SaveLoad.extractFromJValue[CollaborativeFilteringMetaData](tkMetaData)
    val sparkModel: MatrixFactorizationModel = MatrixFactorizationModel.load(sc, path)

    CollaborativeFilteringModel(m.sourceColumnName,
      m.destColumnName,
      m.weightColumnName,
      m.maxSteps,
      m.regularization,
      m.alpha,
      m.numFactors,
      m.useImplicit,
      m.numUserBlocks,
      m.numItemBlock,
      m.checkpointIterations,
      m.targetRMSE,
      m.rank,
      sparkModel)
  }
}

/**
 * Collaborative filtering model
 *
 * @param sourceColumnName source column name.
 * @param destColumnName destination column name.
 * @param weightColumnName weight column name.
 * @param maxSteps max number of super-steps (max iterations) before the algorithm terminates. Default = 10
 * @param regularization float value between 0 .. 1
 * @param alpha double value between 0 .. 1
 * @param numFactors number of the desired factors (rank)
 * @param useImplicit use implicit preference
 * @param numUserBlocks number of user blocks
 * @param numItemBlock number of item blocks
 * @param checkpointIterations Number of iterations between checkpoints
 * @param targetRMSE target RMSE
 * // * @param userFeaturesFrame user features frame
 * // * @param productFeaturesFrame product features frame
 * @param rank rank
 * @param sparkModel model generated using spark
 */
case class CollaborativeFilteringModel(sourceColumnName: String,
                                       destColumnName: String,
                                       weightColumnName: String,
                                       maxSteps: Int,
                                       regularization: Float,
                                       alpha: Double,
                                       numFactors: Int,
                                       useImplicit: Boolean,
                                       numUserBlocks: Int,
                                       numItemBlock: Int,
                                       checkpointIterations: Int,
                                       targetRMSE: Double,
                                       rank: Int,
                                       sparkModel: MatrixFactorizationModel) extends Serializable {

  implicit def rowWrapperToRowWrapperFunctions(rowWrapper: RowWrapper): RowWrapperFunctions = {
    new RowWrapperFunctions(rowWrapper)
  }

  val outputSchema = FrameSchema(Vector(Column("id", DataTypes.int), Column("features", DataTypes.vector(numFactors))))
  lazy val userFrame = {
    val userRowrdd = sparkModel.userFeatures.map {
      case (id, features) => Row(id, DataTypes.toVector(features.length)(features))
    }
    new Frame(userRowrdd, outputSchema)
  }

  lazy val productFrame = {
    val productRowrdd = sparkModel.productFeatures.map {
      case (id, features) => Row(id, DataTypes.toVector(features.length)(features))
    }
    new Frame(productRowrdd, outputSchema)
  }

  /**
   * Collaborative Filtering predict (ALS).
   *
   * @param frame The frame containing the data to predict on
   * @param inputSourceColumnName source column name.
   * @param inputDestColumnName destination column name.
   * @param outputUserColumnName A user column name for the output frame
   * @param outputProductColumnName A product  column name for the output frame
   * @param outputRatingColumnName A rating column name for the output frame
   */
  def createPredictFrame(frame: Frame,
                         inputSourceColumnName: String,
                         inputDestColumnName: String,
                         outputUserColumnName: String = "user",
                         outputProductColumnName: String = "product",
                         outputRatingColumnName: String = "rating"): Frame = {

    require(frame != null, "batch data as a frame is required")

    val frameRdd = new FrameRdd(frame.schema, frame.rdd)
    val schema = frameRdd.schema

    schema.requireColumnIsType(inputSourceColumnName, DataTypes.int)
    schema.requireColumnIsType(inputDestColumnName, DataTypes.int)

    val alsPredictInput = frameRdd.map(edge => {
      (edge.getInt(schema.columnIndex(inputSourceColumnName)),
        edge.getInt(schema.columnIndex(inputDestColumnName))
      )
    })

    val modelRdd: RDD[Rating] = sparkModel.predict(alsPredictInput)

    val newSchema = FrameSchema(Vector(
      Column(outputUserColumnName, DataTypes.int),
      Column(outputProductColumnName, DataTypes.int),
      Column(outputRatingColumnName, DataTypes.float64)))

    val rowRdd = modelRdd.map {
      case alsRating => Row(alsRating.user, alsRating.product, DataTypes.toFloat(alsRating.rating))
    }

    new Frame(rowRdd, newSchema)
    //Cannot do zip because join inside of sparkmodel predict repartitions the rdd
    //val resultFrameRdd = frameRdd.zipFrameRdd(modelFrameRdd)
  }

  /**
   *
   * @param sc
   * @param path
   */
  def save(sc: SparkContext, path: String): Unit = {
    sparkModel.save(sc, path) //saving spark als model
    val formatVersion: Int = 1
    val tkMetadata = CollaborativeFilteringMetaData(sourceColumnName,
      destColumnName,
      weightColumnName,
      maxSteps,
      regularization,
      alpha,
      numFactors,
      useImplicit,
      numUserBlocks,
      numItemBlock,
      checkpointIterations,
      targetRMSE,
      rank)
    TkSaveLoad.saveTk(sc, path, CollaborativeFilteringModel.formatId, formatVersion, tkMetadata)
  }

  /**
   * Collaborative Filtering recommend (ALS).
   *
   * @param entityId A user/product id
   * @param numberOfRecommendations Number of recommendations
   * @param recommendProducts True - products for user; false - users for the product
   * @return Returns an array of recommendations (as array of csv-strings)
   */
  def recommend(entityId: Int, numberOfRecommendations: Int = 1, recommendProducts: Boolean = true): Seq[Map[String, AnyVal]] = {
    val ratingsArray: Array[Rating] = if (recommendProducts) {
      sparkModel.recommendProducts(entityId, numberOfRecommendations)
    }
    else {
      sparkModel.recommendUsers(entityId, numberOfRecommendations)
    }
    formatReturn(ratingsArray)
  }

  //helper function fro recommend
  private def formatReturn(alsRecommendations: Array[Rating]): Seq[Map[String, AnyVal]] = {
    val recommendationAsList =
      for {
        recommendation <- alsRecommendations
        entityId = recommendation.user.asInstanceOf[Int]
        recommendationId = recommendation.product.asInstanceOf[Int]
        rating = recommendation.rating.asInstanceOf[Double]
      } yield Map("user" -> entityId, "product" -> recommendationId, "rating" -> rating)
    recommendationAsList
  }
}

/**
 *
 * @param sourceColumnName source column name.
 * @param destColumnName destination column name.
 * @param weightColumnName weight column name.
 * @param maxSteps max number of super-steps (max iterations) before the algorithm terminates. Default = 10
 * @param regularization float value between 0 .. 1
 * @param alpha double value between 0 .. 1
 * @param numFactors number of the desired factors (rank)
 * @param useImplicit use implicit preference
 * @param numUserBlocks number of user blocks
 * @param numItemBlock number of item blocks
 * @param checkpointIterations Number of iterations between checkpoints
 * @param targetRMSE target RMSE
 * @param rank rank
 */
case class CollaborativeFilteringMetaData private[collaborativefiltering] (sourceColumnName: String,
                                                                           destColumnName: String,
                                                                           weightColumnName: String,
                                                                           maxSteps: Int,
                                                                           regularization: Float,
                                                                           alpha: Double,
                                                                           numFactors: Int,
                                                                           useImplicit: Boolean,
                                                                           numUserBlocks: Int,
                                                                           numItemBlock: Int,
                                                                           checkpointIterations: Int,
                                                                           targetRMSE: Double,
                                                                           rank: Int) extends Serializable