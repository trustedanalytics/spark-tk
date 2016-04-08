package org.trustedanalytics.at.frame.internal.ops.statistics.quantiles

/**
 * Quantile target that will be applied to an element
 * @param quantile quantile. For eg, 40 means 40th quantile
 * @param weight weight that will be applied to the element
 */
case class QuantileTarget(quantile: Double, weight: BigDecimal)
