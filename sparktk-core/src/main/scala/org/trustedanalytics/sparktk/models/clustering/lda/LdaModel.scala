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
package org.trustedanalytics.sparktk.models.clustering.lda

import java.nio.file.{ Files, Path }
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.org.trustedanalytics.sparktk.{ TkLdaModel, LdaModelPredictionResult }
import org.trustedanalytics.scoring.interfaces.{ Model, ModelMetaData, Field }
import org.trustedanalytics.sparktk.TkContext
import org.trustedanalytics.sparktk.frame.internal.RowWrapper
import org.trustedanalytics.sparktk.frame.internal.rdd.RowWrapperFunctions
import org.trustedanalytics.sparktk.frame.{ DataTypes, Frame }
import org.trustedanalytics.sparktk.models.{ ScoringModelUtils, SparkTkModelAdapter }
import org.trustedanalytics.sparktk.saveload.{ SaveLoad, TkSaveLoad, TkSaveableObject }
import scala.language.implicitConversions
import org.json4s.JsonAST.JValue

object LdaModel extends TkSaveableObject {

  /**
   * Creates Latent Dirichlet Allocation model
   * See the discussion about `Latent Dirichlet Allocation at Wikipedia. <http://en.wikipedia.org/wiki/Latent_Dirichlet_allocation>`
   *
   * @param frame Input frame data
   * @param documentColumnName Column Name for documents. Column should contain a str value.
   * @param wordColumnName Column name for words. Column should contain a str value.
   * @param wordCountColumnName Column name for word count. Column should contain an int32 or int64 value.
   * @param maxIterations The maximum number of iterations that the algorithm will execute.
   *                      The valid value range is all positive int. Default is 20.
   * @param alpha  The :term:`hyperparameter` for document-specific distribution over topics.
   *               Mainly used as a smoothing parameter in :term:`Bayesian inference`.
   *               If set to a singleton list List(-1d), then docConcentration is set automatically.
   *               If set to singleton list List(t) where t != -1, then t is replicated to a vector of length k during
   *               LDAOptimizer.initialize(). Otherwise, the alpha must be length k.
   *               Currently the EM optimizer only supports symmetric distributions, so all values in the vector should be the same.
   *               Values should be greater than 1.0. Default value is -1.0 indicating automatic setting.
   * @param beta   The :term:`hyperparameter` for word-specific distribution over topics.
   *               Mainly used as a smoothing parameter in :term:`Bayesian inference`.
   *               Larger value implies that topics contain all words more uniformly and
   *               smaller value implies that topics are more concentrated on a small
   *               subset of words.
   *               Valid value range is all positive float greater than or equal to 1.
   *               Default is 0.1.
   * @param numTopics The number of topics to identify in the LDA model.
   *                  Using fewer topics will speed up the computation, but the extracted topics
   *                  might be more abstract or less specific; using more topics will
   *                  result in more computation but lead to more specific topics.
   *                  Valid value range is all positive int.
   *                  Default is 10.
   * @param seed An optional random seed.
   *                   The random seed is used to initialize the pseudorandom number generator
   *                   used in the LDA model. Setting the random seed to the same value every
   *                   time the model is trained, allows LDA to generate the same topic distribution
   *                   if the corpus and LDA parameters are unchanged.
   * @param checkPointInterval Period (in iterations) between checkpoints (default = 10).
   *                           Checkpointing helps with recovery (when nodes fail). It also helps with eliminating
   *                           temporary shuffle files on disk, which can be important when LDA is run for many
   *                           iterations. If the checkpoint directory is not set, this setting is ignored.
   * @return Trained LdaModel
   */
  def train(frame: Frame,
            documentColumnName: String,
            wordColumnName: String,
            wordCountColumnName: String,
            maxIterations: Int = 20,
            alpha: Option[List[Double]] = None,
            beta: Float = 1.1f,
            numTopics: Int = 10,
            seed: Option[Long] = None,
            checkPointInterval: Int = 10): LdaModel = {

    // validate arguments
    val edgeFrame = frame
    edgeFrame.schema.requireColumnIsType(documentColumnName, DataTypes.string)
    edgeFrame.schema.requireColumnIsType(wordColumnName, DataTypes.string)
    edgeFrame.schema.requireColumnIsType(wordCountColumnName, DataTypes.isIntegerDataType)

    val arguments = LdaTrainArgs(frame,
      documentColumnName,
      wordColumnName,
      wordCountColumnName,
      maxIterations,
      alpha,
      beta,
      numTopics,
      seed,
      checkPointInterval
    )

    val ldaModel: TkLdaModel = LdaTrainFunctions.trainLdaModel(arguments)

    LdaModel(documentColumnName,
      wordColumnName,
      wordCountColumnName,
      maxIterations,
      alpha,
      beta,
      numTopics,
      seed,
      checkPointInterval,
      frame.rowCount(),
      ldaModel)
  }

  def loadTkSaveableObject(sc: SparkContext, path: String, formatVersion: Int, tkMetadata: JValue): Any = {

    validateFormatVersion(formatVersion, 1)
    val m: LdaModelTkMetaData = SaveLoad.extractFromJValue[LdaModelTkMetaData](tkMetadata)
    val sparkModel = TkLdaModel.load(sc, path, LdaModel.formatId, formatVersion)

    LdaModel(m.documentColumnName,
      m.wordColumnName,
      m.wordCountColumnName,
      m.maxIterations,
      m.alpha,
      m.beta,
      m.numTopics,
      m.seed,
      m.checkPointInterval,
      m.trainingDataRowCount,
      sparkModel)
  }

  /**
   * Load an LdaModel from the given path
   * @param tc TkContext
   * @param path location of the model
   * @return LdaModel
   */
  def load(tc: TkContext, path: String): LdaModel = {
    tc.load(path).asInstanceOf[LdaModel]
  }
}

/**
 * LdaModel Class Encapuslates the artifacts necessary to describe LDA Model.
 * The attributes comprise of multiple components\:
 *
 * topics_given_doc: Frame: Conditional probabilities of topic given document.
 * word_given_topics: Frame: Conditional probabilities of word given topic.
 * topics_given_word: Frame: Conditional probabilities of topic given word.
 *
 * These frames are lazily created upon first instantiation
 *
 * @param documentColumnName Column Name for documents. Column should contain a str value.
 * @param wordColumnName Column name for words. Column should contain a str value.
 * @param wordCountColumnName Column name for word count. Column should contain an int32 or int64 value.
 * @param maxIterations The maximum number of iterations that the algorithm will execute.
 *                      The valid value range is all positive int. Default is 20.
 * @param alpha The :term:`hyperparameter` for document-specific distribution over topics.
 *              Mainly used as a smoothing parameter in :term:`Bayesian inference`.
 *              If set to a singleton list List(-1d), then docConcentration is set automatically.
 *              If set to singleton list List(t) where t != -1, then t is replicated to a vector of length k during
 *              LDAOptimizer.initialize(). Otherwise, the alpha must be length k.
 *              Currently the EM optimizer only supports symmetric distributions, so all values in the vector should be the same.
 *              Values should be greater than 1.0. Default value is -1.0 indicating automatic setting.
 * @param beta The :term:`hyperparameter` for word-specific distribution over topics.
 *             Mainly used as a smoothing parameter in :term:`Bayesian inference`.
 *             Larger value implies that topics contain all words more uniformly and
 *             smaller value implies that topics are more concentrated on a small
 *             subset of words.
 *             Valid value range is all positive float greater than or equal to 1.
 *             Default is 0.1.
 * @param numTopics The number of topics to identify in the LDA model.
 *                  Using fewer topics will speed up the computation, but the extracted topics
 *                  might be more abstract or less specific; using more topics will
 *                  result in more computation but lead to more specific topics.
 *                  Valid value range is all positive int.
 *                  Default is 10.
 * @param checkPointInterval Period (in iterations) between checkpoints (default = 10).
 *                           Checkpointing helps with recovery (when nodes fail). It also helps with eliminating
 *                           temporary shuffle files on disk, which can be important when LDA is run for many
 *                           iterations. If the checkpoint directory is not set, this setting is ignored.
 * @param seed An optional random seed.
 *                   The random seed is used to initialize the pseudorandom number generator
 *                   used in the LDA model. Setting the random seed to the same value every
 *                   time the model is trained, allows LDA to generate the same topic distribution
 *                   if the corpus and LDA parameters are unchanged.
 * @param sparkModel Trained Spark LDA Model (TkLdaModel)
 */
case class LdaModel private[lda] (documentColumnName: String,
                                  wordColumnName: String,
                                  wordCountColumnName: String,
                                  maxIterations: Int,
                                  alpha: Option[List[Double]],
                                  beta: Float,
                                  numTopics: Int,
                                  seed: Option[Long],
                                  checkPointInterval: Int,
                                  trainingDataRowCount: Long,
                                  sparkModel: TkLdaModel) extends Serializable with Model {

  implicit def rowWrapperToRowWrapperFunctions(rowWrapper: RowWrapper): RowWrapperFunctions = {
    new RowWrapperFunctions(rowWrapper)
  }

  /* LDA frame with conditional probabilities of topics given document */
  lazy val topicsGivenDocFrame = new Frame(sparkModel.getTopicsGivenDocFrame)

  /* LDA frame with conditional probabilities of word given topics */
  lazy val wordGivenTopicsFrame = new Frame(sparkModel.getWordGivenTopicsFrame)

  /* LDA frame with conditional probabilities of topics given word */
  lazy val topicsGivenWordFrame = new Frame(sparkModel.getTopicsGivenWordFrame)

  /* The configuration and learning curve report for Latent Dirichlet Allocation as a multiple line str */
  lazy val report = sparkModel.getModelSummary(trainingDataRowCount, maxIterations)

  /* Return the topic probabilities based on trained LDA Model for the documents */
  def predict(document: List[String]): LdaModelPredictionResult = {
    sparkModel.predict(document)
  }

  /**
   * Saves this model to a file
   * @param sc active SparkContext
   * @param path save to path
   */
  def save(sc: SparkContext, path: String): Unit = {
    val formatVersion: Int = 1
    sparkModel.save(sc, path, LdaModel.formatId, formatVersion)
    val tkMetadata = LdaModelTkMetaData(documentColumnName,
      wordColumnName,
      wordCountColumnName,
      maxIterations,
      alpha,
      beta,
      numTopics,
      seed,
      checkPointInterval,
      trainingDataRowCount)
    TkSaveLoad.saveTk(sc, path, LdaModel.formatId, formatVersion, tkMetadata)
  }

  /**
   * gets the prediction on the provided record
   * @param row a record that needs to be predicted on
   * @return the row along with its prediction
   */
  def score(row: Array[Any]): Array[Any] = {
    val inputDocument = row.flatMap {
      case list: List[_] => list.map(_.toString)
      case _ => throw new IllegalArgumentException("Scoring input must be a list of words")
    }
    val predictReturn = predict(inputDocument.toList)
    row :+ predictReturn.topicsGivenDoc :+ predictReturn.newWordsCount :+ predictReturn.newWordsPercentage
  }

  /**
   * @return fields containing the input names and their datatypes
   */
  def input(): Array[Field] = {
    val input = Array[Field](Field(documentColumnName, "Array[String]"))
    input
  }

  /**
   * @return fields containing the input names and their datatypes along with the output and its datatype
   */
  def output(): Array[Field] = {
    var output = input()
    output = output :+ Field("topics_given_doc", "Vector[Double]")
    output = output :+ Field("new_words_count", "Int")
    output :+ Field("new_words_percentage", "Double")
  }

  /**
   * @return metadata about the model
   */
  def modelMetadata(): ModelMetaData = {
    //todo provide an API for the user to populate the custom metadata fields
    new ModelMetaData("Lda Model", classOf[LdaModel].getName, classOf[SparkTkModelAdapter].getName, Map())
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
      ScoringModelUtils.saveToMar(marSavePath, classOf[LdaModel].getName, tmpDir)
    }
    finally {
      sys.addShutdownHook(FileUtils.deleteQuietly(tmpDir.toFile)) // Delete temporary directory on exit
    }
  }
}

/* Class to store LdaModel Metadata */
case class LdaModelTkMetaData(documentColumnName: String,
                              wordColumnName: String,
                              wordCountColumnName: String,
                              maxIterations: Int,
                              alpha: Option[List[Double]],
                              beta: Float,
                              numTopics: Int,
                              seed: Option[Long],
                              checkPointInterval: Int,
                              trainingDataRowCount: Long) extends Serializable

