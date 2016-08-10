package org.trustedanalytics.sparktk.frame.internal.rdd

/**
 * Frequency of scores and corresponding labels in model predictions
 *
 * @param score Score or prediction by model
 * @param label Ground-truth label
 * @param frequency Frequency of predictions that match score and label
 * @tparam T Type of score and label
 */
case class ScoreAndLabel[T](score: T, label: T, frequency: Long = 1)