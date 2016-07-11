package org.apache.spark.mllib.clustering

/**
 * Return arguments to the LDA predict plugin
 *
 * @param topicsGivenDoc Vector of conditional probabilities of topics given document
 * @param newWordsCount Count of new words in test document not present in training set
 * @param newWordsPercentage Percentage of new word in test document
 */
case class LdaModelPredictionResult(topicsGivenDoc: Vector[Double],
                                    newWordsCount: Int,
                                    newWordsPercentage: Double)
