package org.trustedanalytics.sparktk.frame.internal.ops.statistics.descriptives

import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameSummarization, BaseFrame }
import org.trustedanalytics.sparktk.frame.DataTypes.DataType

trait ColumnSummaryStatisticsSummarization extends BaseFrame {

  def columnSummaryStatistics(dataColumn: String,
                              weightsColumn: Option[String],
                              usePopulationVariance: Boolean = false): ColumnSummaryStatisticsReturn = {
    execute(ColumnSummaryStatistics(dataColumn, weightsColumn, usePopulationVariance))
  }
}

/**
 * Calculate multiple statistics for a column.
 *
 * @param dataColumn The column to be statistically summarized.
 *                   Must contain numerical data; all NaNs and infinite values are excluded from the calculation.
 * @param weightsColumn Name of column holding weights of column values.
 * @param usePopulationVariance If true, the variance is calculated as the population variance.
 *                              If false, the variance calculated as the sample variance.
 *                              Because this option affects the variance, it affects the standard deviation and
 *                              the confidence intervals as well.
 *                              Default is false.
 */
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