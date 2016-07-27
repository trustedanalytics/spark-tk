package org.trustedanalytics.sparktk.models.clustering.lda

import org.apache.spark.mllib.clustering.org.trustedanalytics.sparktk.TkLdaModel
import org.apache.spark.mllib.clustering.{ DistributedLDAModel, LDA }
import org.apache.spark.mllib.linalg.{ Vectors, Vector }
import org.apache.spark.rdd.RDD
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd

object LdaTrainFunctions extends Serializable {

  /**
   * Train LDA model
   *
   * @param args LDA train arguments
   * @return Trained LDA model
   */
  def trainLdaModel(args: LdaTrainArgs): TkLdaModel = {
    val ldaCorpus = LdaCorpus(args)
    val trainCorpus = ldaCorpus.createCorpus().cache()
    val distLdaModel = runLda(trainCorpus, args)

    val ldaModel = new TkLdaModel(distLdaModel, args.documentColumnName, args.wordColumnName)
    ldaModel.setDocTopicFrame(trainCorpus, args.documentColumnName, "topic_probabilities")
    ldaModel.setWordTopicFrames(
      ldaCorpus.uniqueWordsFrame,
      ldaCorpus.wordIdAssigner.ldaWordIdColumnName,
      ldaCorpus.wordIdAssigner.ldaWordColumnName,
      ldaCorpus.wordIdAssigner.ldaWordCountColumnName,
      args.wordColumnName,
      "topic_probabilities"
    )
    ldaModel
  }

  /**
   * Initialize LDA runner with training arguments supplied by user
   *
   * @param args LDA train arguments
   * @return LDA runner
   */
  private[clustering] def initializeLdaRunner(args: LdaTrainArgs): LDA = {
    val ldaRunner = new LDA()
    val alpha: Vector = Vectors.dense(args.getAlpha.toArray)
    ldaRunner.setDocConcentration(alpha)
    ldaRunner.setTopicConcentration(args.beta)
    ldaRunner.setMaxIterations(args.maxIterations)
    ldaRunner.setK(args.numTopics)
    if (args.randomSeed.isDefined) {
      ldaRunner.setSeed(args.randomSeed.get)
    }
    ldaRunner
  }

  /**
   * Run LDA model
   * @param corpus LDA corpus of document Ids, document names, and corresponding word count vectors.
   *               The length of each word count vector is the vocabulary size.
   * @param args LDA train arguments
   * @return Trained LDA model
   */
  private[clustering] def runLda(corpus: RDD[(Long, (String, Vector))], args: LdaTrainArgs): DistributedLDAModel = {
    val ldaCorpus = corpus.map { case ((documentId, (document, wordVector))) => (documentId, wordVector) }
    val ldaModel = initializeLdaRunner(args).run(ldaCorpus)
    ldaModel match {
      case m: DistributedLDAModel => m
      case _ => throw new RuntimeException("Local LDA models are not supported.")
    }
  }
}
