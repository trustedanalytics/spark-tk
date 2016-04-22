package org.trustedanalytics.sparktk.frame.internal.ops.statistics.quantiles

/**
 * Quantile composing element which contains element's index and its weight
 * @param index element index
 * @param quantileTarget the quantile target that the element can be applied to
 */
case class QuantileComposingElement(index: Long, quantileTarget: QuantileTarget)
