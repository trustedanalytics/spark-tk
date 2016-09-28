package org.trustedanalytics.sparktk.frame.internal.ops.statistics.descriptives

import org.trustedanalytics.sparktk.frame.DataTypes.DataType
import org.apache.spark.sql.Row
import org.trustedanalytics.sparktk.frame.internal.ops.statistics.numericalstatistics._
import org.trustedanalytics.sparktk.frame.internal.ops.statistics.{ FrequencyStatistics, OrderStatistics }
import org.apache.spark.rdd.RDD

/**
 * Provides functions for taking statistics on column data.
 */
object ColumnStatistics extends Serializable {

  /**
   * Calculate (weighted) mode of a data column, the weight of the mode, and the total weight of the column.
   * A mode is a value that has maximum weight. Ties are resolved arbitrarily.
   * Values with non-positive weights (including NaNs and infinite values) are thrown out before the calculation is
   * performed.
   *
   * When the total weight is 0, the option None is given for the mode and the weight of the mode.
   *
   * @param dataColumnIndex Index of the column providing data.
   * @param dataType The type of the data column.
   * @param weightsColumnIndexAndType Option for index and data type of column providing weights. Must be numerical data.
   * @param modeCountOption Option for the maximum number of modes returned. Defaults to 1.
   * @param rowRDD RDD of input rows.
   * @return The mode of the column (as a string), the weight of the mode, and the total weight of the data.
   */
  def columnMode(dataColumnIndex: Int,
                 dataType: DataType,
                 weightsColumnIndexAndType: Option[(Int, DataType)],
                 modeCountOption: Option[Int],
                 rowRDD: RDD[Row]): ColumnModeReturn = {

    val defaultNumberOfModesReturned = 1

    val dataWeightPairs: RDD[(Any, Double)] =
      getDataWeightPairs(dataColumnIndex, weightsColumnIndexAndType, rowRDD)

    val modeCount = modeCountOption.getOrElse(defaultNumberOfModesReturned)

    val frequencyStatistics = new FrequencyStatistics(dataWeightPairs, modeCount)
    ColumnModeReturn(frequencyStatistics.modeSet.toArray[Any],
      frequencyStatistics.weightOfMode,
      frequencyStatistics.totalWeight,
      frequencyStatistics.modeCount)
  }

  /**
   * Calculate the median of a data column containing numerical data. The median is the least value X in the range of the
   * distribution so that the cumulative weight strictly below X is < 1/2  the total weight and the cumulative
   * distribution up to and including X is >= 1/2 the total weight.
   *
   * Values with non-positive weights(including NaNs and infinite values) are thrown out before the calculation is
   * performed. The option None is returned when the total weight is 0.
   *
   * @param dataColumnIndex Index of the data column.
   * @param dataType The type of the data column.
   * @param weightsColumnIndexAndType  Option for index and data type of column providing weights. Must be numerical data.
   * @param rowRDD RDD of input rows.
   * @return The median of the column.
   */
  def columnMedian(dataColumnIndex: Int,
                   dataType: DataType,
                   weightsColumnIndexAndType: Option[(Int, DataType)],
                   rowRDD: RDD[Row]): Option[Any] = {

    val dataWeightPairs: RDD[(Any, Double)] =
      getDataWeightPairs(dataColumnIndex, weightsColumnIndexAndType, rowRDD)

    implicit val ordering: Ordering[Any] = new NumericalOrdering(dataType)

    val orderStatistics = new OrderStatistics[Any](dataWeightPairs)

    orderStatistics.medianOption
  }

  private class NumericalOrdering(dataType: DataType) extends Ordering[Any] {
    override def compare(x: Any, y: Any): Int = {
      dataType.asDouble(x).compareTo(dataType.asDouble(y))
    }
  }

  /**
   * Calculate summary statistics of data column, possibly weighted by an optional weights column.
   *
   * Values with non-positive weights(including NaNs and infinite values) are thrown out before the calculation is
   * performed, however, they are logged as "bad rows" (when a row contain a datum or a weight that is not a finite
   * number) or as "non positive weight" (when a row's weight entry is <= 0).
   *
   * @param dataColumnIndex Index of column providing the data. Must be numerical data.
   * @param dataType The type of the data column.
   * @param weightsColumnIndexOption Option for index of column providing the weights. Must be numerical data.
   * @param weightsTypeOption Option for the datatype of the weights.
   * @param rowRDD RDD of input rows.
   * @param usePopulationVariance If true, variance is calculated as population variance. If false, variance is
   *                              calculated as sample variance.
   * @return Summary statistics of the column.
   */
  def columnSummaryStatistics(dataColumnIndex: Int,
                              dataType: DataType,
                              weightsColumnIndexOption: Option[Int],
                              weightsTypeOption: Option[DataType],
                              rowRDD: RDD[Row],
                              usePopulationVariance: Boolean): ColumnSummaryStatisticsReturn = {

    val dataWeightPairs: RDD[(Option[Double], Option[Double])] =
      getDoubleWeightPairs(dataColumnIndex, dataType, weightsColumnIndexOption, weightsTypeOption, rowRDD)

    val stats = new NumericalStatistics(dataWeightPairs, usePopulationVariance)

    ColumnSummaryStatisticsReturn(mean = stats.weightedMean,
      geometricMean = stats.weightedGeometricMean,
      variance = stats.weightedVariance,
      standardDeviation = stats.weightedStandardDeviation,
      totalWeight = stats.totalWeight,
      meanConfidenceLower = stats.meanConfidenceLower,
      meanConfidenceUpper = stats.meanConfidenceUpper,
      minimum = stats.min,
      maximum = stats.max,
      positiveWeightCount = stats.positiveWeightCount,
      nonPositiveWeightCount = stats.nonPositiveWeightCount,
      badRowCount = stats.badRowCount,
      goodRowCount = stats.goodRowCount)
  }

  def getDataWeightPairs(dataColumnIndex: Int,
                         weightColumnIndexAndType: Option[(Int, DataType)],
                         rowRDD: RDD[Row]): RDD[(Any, Double)] = {

    val dataRDD: RDD[Any] = rowRDD.map(row => row(dataColumnIndex))

    val weighted = weightColumnIndexAndType.isDefined

    val weightsRDD = if (weighted) {
      val weightColumnIndex = weightColumnIndexAndType.get._1
      val weightColumnDataType = weightColumnIndexAndType.get._2
      rowRDD.map(row => weightColumnDataType.asDouble(row(weightColumnIndex)))
    }
    else
      null

    if (weighted) dataRDD.zip(weightsRDD) else dataRDD.map(x => (x, 1.toDouble))
  }

  private def getDoubleWeightPairs(dataColumnIndex: Int,
                                   dataType: DataType,
                                   weightsColumnIndexOption: Option[Int],
                                   weightsTypeOption: Option[DataType],
                                   rowRDD: RDD[Row]): RDD[(Option[Double], Option[Double])] = {

    val dataRDD: RDD[Option[Double]] = rowRDD.map {
      case row => extractColumnValueAsDoubleFromRow(row, dataColumnIndex, dataType)
    }

    val weighted = weightsColumnIndexOption.isDefined

    if (weightsColumnIndexOption.nonEmpty && weightsTypeOption.isEmpty) {
      throw new IllegalArgumentException("Cannot specify weights column without specifying its datatype.")
    }

    val weightsRDD = if (weighted)
      rowRDD.map {
        case row => extractColumnValueAsDoubleFromRow(row, weightsColumnIndexOption.get, weightsTypeOption.get)
      }
    else
      null

    if (weighted) dataRDD.zip(weightsRDD) else dataRDD.map(x => (x, Some(1.toDouble)))
  }

  private def extractColumnValueAsDoubleFromRow(row: Row, columnIndex: Int, dataType: DataType): Option[Double] = {
    val columnValue = row(columnIndex)
    columnValue match {
      case null => None
      case value => Some(dataType.asDouble(value))
    }
  }

}
