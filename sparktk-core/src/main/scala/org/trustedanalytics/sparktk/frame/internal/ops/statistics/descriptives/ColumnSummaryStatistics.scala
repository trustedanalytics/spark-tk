package org.trustedanalytics.sparktk.frame.internal.ops.statistics.descriptives

import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameSummarization, BaseFrame }
import org.trustedanalytics.sparktk.frame.DataTypes.DataType

trait ColumnSummaryStatisticsSummarization extends BaseFrame {
  /**
   * Calculate multiple statistics for a column.
   *
   * @note
   *       ==Sample Variance==
   *       Sample Variance is computed by the following formula:
   *
   *       .. math::
   *
   *       \left( \frac{1}{W - 1} \right) * sum_{i} \
   *       \left(x_{i} - M \right) ^{2}
   *
   *       where :math:`W` is sum of weights over valid elements of positive weight, and :math:`M` is the weighted mean.
   *
   *       '''Population Variance'''
   *
   *       Population Variance is computed by the following formula:
   *
   *       .. math::
   *
   *       \left( \frac{1}{W} \right) * sum_{i} \
   *       \left(x_{i} - M \right) ^{2}
   *
   *       where :math:`W` is sum of weights over valid elements of positive weight, and :math:`M` is the weighted mean.
   *
   *       '''Standard Deviation'''
   *
   *       The square root of the variance.
   *
   *       '''Logging Invalid Data'''
   *
   *       A row is bad when it contains a NaN or infinite value in either its data or weights column. In this case,
   *       it contributes to bad_row_count; otherwise it contributes to good row count.
   *
   *       A good row can be skipped because the value in its weight column is less than or equal to 0. In this case,
   *       it contributes to non_positive_weight_count, otherwise (when the weight is greater than 0) it contributes to
   *       valid_data_weight_pair_count.
   *
   *       '''Equations'''
   *
   *       {bad_row_count + good_row_count = # rows in the frame
   *       positive_weight_count + non_positive_weight_count = good_row_count}
   *
   *       In particular, when no weights column is provided and all weights are 1.0:
   *
   *       {non_positive_weight_count = 0 and
   *       positive_weight_count = good_row_count}
   *
   * @param dataColumn The column to be statistically summarized.
   *                   Must contain numerical data; all NaNs and infinite values are excluded from the calculation.
   * @param weightsColumn Name of column holding weights of column values.
   * @param usePopulationVariance If true, the variance is calculated as the population variance.
   *                              If false, the variance calculated as the sample variance.
   *                              Because this option affects the variance, it affects the standard deviation and
   *                              the confidence intervals as well.
   *                              Default is false.
   * @return The data returned is contained in a ColumnSummaryStatisticsReturn case class:
   *         - mean : ''Double''<br>Arithmetic mean of the data.
   *         - geometricMean : ''Double''<br>Geometric mean of the data. None when there is a data element
   *         <= 0, 1.0 when there are no data elements.
   *         - variance : ''Double''<br>None when there are <= 1 many data elements. Sample variance is the
   *         weighted sum of the squared distance of each data element from the weighted mean, divided by the total
   *         weight minus 1. None when the sum of the weights is <= 1. Population variance is the weighted sum of
   *         the squared distance of each data element from the weighted mean, divided by the total weight.
   *         - standardDeviation : ''Double''<br>The square root of the variance. None when  sample variance
   *         is being used and the sum of weights is <= 1.
   *         - totalWeight : long<br>The count of all data elements that are finite numbers. In other words, after
   *         excluding NaNs and infinite values.
   *         - minimum : ''Double''<br>Minimum value in the data. None when there are no data elements.
   *         - maximum : ''Double''<br>Maximum value in the data. None when there are no data elements.
   *         - meanConfidenceLower : ''Double''<br>Lower limit of the 95% confidence interval about the mean.
   *         Assumes a Gaussian distribution. None when there are no elements of positive weight.
   *         - meanConfidenceUpper : ''Double''<br>Upper limit of the 95% confidence interval about the mean.
   *         Assumes a Gaussian distribution. None when there are no elements of positive weight.
   *         - badRowCount : ''Long''<br>The number of rows containing a NaN or infinite value in either
   *         the data or weights column.
   *         - goodRowCount : ''Long''<br>The number of rows not containing a NaN or infinite value in
   *         either the data or weights column.
   *         - positiveWeightCount : ''Long''<br>The number of valid data elements with weight > 0. This
   *         is the number of entries used in the statistical calculation.
   *         - nonPositiveWeightCount : ''Long'' <br>The number valid data elements with finite weight < 0.
   */
  def columnSummaryStatistics(dataColumn: String,
                              weightsColumn: Option[String],
                              usePopulationVariance: Boolean = false): ColumnSummaryStatisticsReturn = {
    execute(ColumnSummaryStatistics(dataColumn, weightsColumn, usePopulationVariance))
  }
}

case class ColumnSummaryStatistics(dataColumn: String,
                                   weightsColumn: Option[String] = None,
                                   usePopulationVariance: Boolean = false) extends FrameSummarization[ColumnSummaryStatisticsReturn] {
  require(dataColumn != null, "data column is required but not provided")

  override def work(state: FrameState): ColumnSummaryStatisticsReturn = {
    val (weightsColumnIndexOption, weightsDataTypeOption) = if (weightsColumn.isEmpty) {
      (None, None)
    }
    else {
      val weightsColumnIndex = state.schema.columnIndex(weightsColumn.get)
      (Some(weightsColumnIndex), Some(state.schema.columnDataType(weightsColumn.get)))
    }

    // run the operation and return the results
    ColumnStatistics.columnSummaryStatistics(
      state.schema.columnIndex(dataColumn),
      state.schema.columnDataType(dataColumn),
      weightsColumnIndexOption,
      weightsDataTypeOption,
      state.rdd,
      usePopulationVariance)
  }

}

/**
 * Summary statistics for a dataframe column. All values are optionally weighted by a second column. If no weights are
 * provided, all elements receive a uniform weight of 1. If any element receives a weight <= 0, that element is thrown
 * out of the calculation. If a row contains a NaN or infinite value in either the data or weights column, that row is
 * skipped and a count of bad rows is incremented.
 *
 *
 * @param mean Arithmetic mean of the data.
 * @param geometricMean Geometric mean of the data. None when there is a non-positive data element, 1 if there are no
 *                       data elements.
 * @param variance If sample variance is used,  the variance  is the weighted sum of squared distances from the mean is
 *                 divided by the sum of weights minus 1 (NaN if the sum of the weights is <= 1).
 *                 If population variance is used, the variance is the weighted sum of squared distances from the mean
 *                 divided by the sum of weights.
 * @param standardDeviation Square root of the variance. None when sample variance is used and the sum of the weights
 *                          is <= 1.
 * @param totalWeight The sum of all weights over valid input rows. (Ie. neither data nor weight is NaN, or infinity,
 *                    and weight is > 0).
 * @param minimum Minimum value in the data. None when there are no data elements of positive weight.
 * @param maximum Maximum value in the data. None when there are no data elements of positive weight.
 * @param meanConfidenceLower: Lower limit of the 95% confidence interval about the mean. Assumes a Gaussian RV.
 *                             None when there are no elements of positive weight.
 * @param meanConfidenceUpper: Upper limit of the 95% confidence interval about the mean. Assumes a Gaussian RV.
 *                             None when there are no elements of positive weight.
 * @param badRowCount The number of rows containing a NaN or infinite value in either the data or weights column.
 * @param goodRowCount The number of rows containing a NaN or infinite value in either the data or weight
 * @param positiveWeightCount  The number valid data elements with weights > 0.
 *                             This is the number of entries used for the calculation of the statistics.
 * @param nonPositiveWeightCount The number valid data elements with weight <= 0.
 */
case class ColumnSummaryStatisticsReturn(mean: Double,
                                         geometricMean: Double,
                                         variance: Double,
                                         standardDeviation: Double,
                                         totalWeight: Double,
                                         minimum: Double,
                                         maximum: Double,
                                         meanConfidenceLower: Double,
                                         meanConfidenceUpper: Double,
                                         badRowCount: Long,
                                         goodRowCount: Long,
                                         positiveWeightCount: Long,
                                         nonPositiveWeightCount: Long)