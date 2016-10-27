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
package org.trustedanalytics.sparktk.models.collaborativefiltering.collaborative_filtering

import java.io.{ FileOutputStream, File }
import java.nio.file.{ Files, Path }

import org.apache.commons.io.{ IOUtils, FileUtils }
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{ Vector => MllibVector }
import org.apache.spark.mllib.recommendation.{ MatrixFactorizationModel, ALS, Rating }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.json4s.JsonAST.JValue
import org.trustedanalytics.scoring.interfaces.{ Model, ModelMetaData, Field }
import org.trustedanalytics.sparktk.TkContext
import org.trustedanalytics.sparktk.frame.internal.RowWrapper
import org.trustedanalytics.sparktk.frame.internal.rdd.{ VectorUtils, FrameRdd, RowWrapperFunctions }
import org.trustedanalytics.sparktk.frame._
import org.trustedanalytics.sparktk.models.{ ScoringModelUtils, TkSearchPath, SparkTkModelAdapter }
import org.trustedanalytics.sparktk.saveload.{ SaveLoad, TkSaveLoad, TkSaveableObject }

object CollaborativeFilteringModel extends TkSaveableObject {

  /**
   *
   * @param frame The frame containing the data to train on
   * @param sourceColumnName source column name.
   * @param destColumnName destination column name.
   * @param weightColumnName weight column name.
   * @param maxSteps max number of super-steps (max iterations) before the algorithm terminates.
   * @param regularization float value between 0 .. 1
   * @param alpha double value between 0 .. 1
   * @param numFactors number of the desired factors (rank)
   * @param useImplicit use implicit preference
   * @param numUserBlocks number of user blocks
   * @param numItemBlocks number of item blocks
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
            numItemBlocks: Int = 3,
            checkpointIterations: Int = 10,
            targetRMSE: Double = 0.05): CollaborativeFilteringModel = {

    require(frame != null, "input frame is required")
    require(StringUtils.isNotEmpty(sourceColumnName), "source column name is required")
    require(StringUtils.isNotEmpty(destColumnName), "destination column name is required")
    require(StringUtils.isNotEmpty(weightColumnName), "weight column name is required")
    require(maxSteps > 1, "max steps must be a positive integer")
    require(regularization > 0 && regularization < 1, "regularization must be a positive value between 0 and 1")
    require(alpha > 0 & alpha < 1, "alpha must be a positive value between 0 and 1")
    require(checkpointIterations > 0, "Iterations between checkpoints must be positive")
    require(numFactors > 0, "number of factors must be a positive integer")
    require(numUserBlocks > 0, "number of user blocks must be a positive integer")
    require(numItemBlocks > 0, "number of item blocks must be a positive integer")
    require(targetRMSE > 0, "target RMSE must be a positive value")

    val schema = frame.schema
    val frameRdd = new FrameRdd(frame.schema, frame.rdd)

    schema.requireColumnIsType(sourceColumnName, DataTypes.int)
    schema.requireColumnIsType(destColumnName, DataTypes.int)
    schema.requireColumnIsType(weightColumnName, DataTypes.float64)
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
      numItemBlocks,
      checkpointIterations,
      targetRMSE,
      alsTrainedModel.rank,
      alsTrainedModel)
  }

  def loadTkSaveableObject(sc: SparkContext, path: String, formatVersion: Int, tkMetaData: JValue): Any = {
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

  /**
   * Load a PcaModel from the given path
   *
   * @param tc TkContext
   * @param path location
   * @return
   */
  def load(tc: TkContext, path: String): CollaborativeFilteringModel = {
    tc.load(path).asInstanceOf[CollaborativeFilteringModel]
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
                                       sparkModel: MatrixFactorizationModel) extends Serializable with Model {

  implicit def rowWrapperToRowWrapperFunctions(rowWrapper: RowWrapper): RowWrapperFunctions = {
    new RowWrapperFunctions(rowWrapper)
  }

  private lazy val outputSchema = FrameSchema(Vector(Column("id", DataTypes.int), Column("features", DataTypes.vector(numFactors))))
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
  def predict(frame: Frame,
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
   * Predicts the rating of one user for one product.
   * @param row a record that needs to be predicted on (user and product integers)
   * @return the row along with its prediction
   */
  def score(row: Array[Any]): Array[Any] = {
    require(row != null && row.length == 2, "Input row must have two integers (for user and product).")
    val user = ScoringModelUtils.asInt(row(0))
    val product = ScoringModelUtils.asInt(row(1))
    row :+ sparkModel.predict(user, product)
  }

  /**
   * @return fields containing the input names and their datatypes
   */
  def input(): Array[Field] = {
    Array[Field](Field("user", "Int"), Field("product", "Int"))
  }

  /**
   * @return fields containing the input names and their datatypes along with the output and its datatype
   */
  def output(): Array[Field] = {
    val output = input()
    output :+ Field("Score", "Double")
  }

  /**
   * @return metadata about the model
   */
  def modelMetadata(): ModelMetaData = {
    //todo provide a for the user to populate the custom metadata fields
    new ModelMetaData("Collaborative Filtering Model", classOf[CollaborativeFilteringModel].getName, classOf[SparkTkModelAdapter].getName, Map())
  }

  /**
   * @param sc active SparkContext
   * @param marSavePath location where the MAR file needs to be saved
   * @return full path to the location of the MAR file
   */
  def exportToMar(sc: SparkContext, marSavePath: String): String = {
    var tmpDir: Path = null
    try {
      tmpDir = Files.createTempDirectory("sparktk-scoring-model")
      save(sc, tmpDir.toString)
      ScoringModelUtils.saveToMar(marSavePath, classOf[CollaborativeFilteringModel].getName, tmpDir)
    }
    finally {
      sys.addShutdownHook(FileUtils.deleteQuietly(tmpDir.toFile)) // Delete temporary directory on exit
    }
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