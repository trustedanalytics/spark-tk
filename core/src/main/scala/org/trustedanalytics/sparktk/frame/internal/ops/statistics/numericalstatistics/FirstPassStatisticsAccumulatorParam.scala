package org.trustedanalytics.sparktk.frame.internal.ops.statistics.numericalstatistics

import org.apache.spark.AccumulatorParam

/**
 * Accumulator param for combiningin FirstPassStatistics.
 */
private[numericalstatistics] class FirstPassStatisticsAccumulatorParam
    extends AccumulatorParam[FirstPassStatistics] with Serializable {

  override def zero(initialValue: FirstPassStatistics) =
    FirstPassStatistics(mean = 0,
      weightedSumOfSquares = 0,
      weightedSumOfSquaredDistancesFromMean = 0,
      weightedSumOfLogs = Some(0),
      minimum = Double.PositiveInfinity,
      maximum = Double.NegativeInfinity,
      totalWeight = 0,
      positiveWeightCount = 0,
      nonPositiveWeightCount = 0,
      badRowCount = 0,
      goodRowCount = 0)

  override def addInPlace(stats1: FirstPassStatistics, stats2: FirstPassStatistics): FirstPassStatistics = {

    val totalWeight = stats1.totalWeight + stats2.totalWeight

    val mean = if (totalWeight > BigDecimal(0))
      (stats1.mean * stats1.totalWeight + stats2.mean * stats2.totalWeight) / totalWeight
    else
      BigDecimal(0)

    val weightedSumOfSquares = stats1.weightedSumOfSquares + stats2.weightedSumOfSquares

    val sumOfSquaredDistancesFromMean =
      weightedSumOfSquares - BigDecimal(2) * mean * mean * totalWeight + mean * mean * totalWeight

    val weightedSumOfLogs: Option[BigDecimal] =
      if (stats1.weightedSumOfLogs.nonEmpty && stats2.weightedSumOfLogs.nonEmpty) {
        Some(stats1.weightedSumOfLogs.get + stats2.weightedSumOfLogs.get)
      }
      else {
        None
      }

    FirstPassStatistics(mean = mean,
      weightedSumOfSquares = weightedSumOfSquares,
      weightedSumOfSquaredDistancesFromMean = sumOfSquaredDistancesFromMean,
      weightedSumOfLogs = weightedSumOfLogs,
      minimum = Math.min(stats1.minimum, stats2.minimum),
      maximum = Math.max(stats1.maximum, stats2.maximum),
      totalWeight = totalWeight,
      positiveWeightCount = stats1.positiveWeightCount + stats2.positiveWeightCount,
      nonPositiveWeightCount = stats1.nonPositiveWeightCount + stats2.nonPositiveWeightCount,
      badRowCount = stats1.badRowCount + stats2.badRowCount,
      goodRowCount = stats1.goodRowCount + stats2.goodRowCount)
  }
}
