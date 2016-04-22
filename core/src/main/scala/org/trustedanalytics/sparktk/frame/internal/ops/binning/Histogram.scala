package org.trustedanalytics.sparktk.frame.internal.ops.binning

import org.trustedanalytics.sparktk.frame.DataTypes.DataType
import org.trustedanalytics.sparktk.frame.internal.{ BaseFrame, FrameSummarization, FrameState }
import org.trustedanalytics.sparktk.frame.DataTypes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

trait HistogramSummarization extends BaseFrame {
  def histogram(column: String,
                numBins: Option[Int] = None,
                weightColumnName: Option[String] = None,
                binType: String = "equalwidth"): Map[String, Seq[Double]] = {
    execute(Histogram(column, numBins, weightColumnName, binType))
  }
}

case class Histogram(column: String,
                     numBins: Option[Int] = None,
                     weightColumnName: Option[String] = None,
                     binType: String) extends FrameSummarization[Map[String, Seq[Double]]] {

  override def work(state: FrameState): Map[String, Seq[Double]] = {
    val columnIndex: Int = state.schema.columnIndex(column)
    val columnType = state.schema.columnDataType(column)
    require(columnType.isNumerical, s"Invalid column ${column} for histogram.  Expected a numerical data type, but got $columnType.")
    require(binType == "equalwidth" || binType == "equaldepth", s"bin type must be 'equalwidth' or 'equaldepth', not ${binType}.")
    if (numBins.isDefined)
      require(numBins.get > 0, "the number of bins must be greater than 0")

    val weightColumnIndex: Option[Int] = weightColumnName match {
      case Some(n) =>
        val columnType = state.schema.columnDataType(n)
        require(columnType.isNumerical, s"Invalid column $n for bin column.  Expected a numerical data type, but got $columnType.")
        Some(state.schema.columnIndex(n))
      case None => None
    }

    val computedNumBins: Int = HistogramFunctions.getNumBins(numBins, state.rdd)

    computeHistogram(state.rdd, columnIndex, weightColumnIndex, computedNumBins, binType == "equalwidth")
  }

  /**
   * compute histogram information from column in a dataFrame
   * @param dataFrame rdd containing the required information
   * @param columnIndex index of column to compute information against
   * @param weightColumnIndex optional index of a column containing the weighted value of each record. Must be numeric,
   *                          will assume to equal 1 if not included
   * @param numBins number of bins to compute
   * @param equalWidth true if we are using equalwidth binning false if not
   * @return a map containing the cutoffs (list containing the edges of each bin), hist (list containing count of
   *         the weighted observations found in each bin), and density (list containing a decimal containing the
   *         percentage of observations found in the total set per bin).
   */
  private[binning] def computeHistogram(dataFrame: RDD[Row],
                                        columnIndex: Int,
                                        weightColumnIndex: Option[Int],
                                        numBins: Int,
                                        equalWidth: Boolean = true): Map[String, Seq[Double]] = {
    val binnedResults = if (equalWidth)
      DiscretizationFunctions.binEqualWidth(columnIndex, numBins, dataFrame)
    else
      DiscretizationFunctions.binEqualDepth(columnIndex, numBins, weightColumnIndex, dataFrame)

    //get the size of each observation in the rdd. if it is negative do not count the observation
    //todo: warn user if a negative weight appears
    val pairedRDD: RDD[(Int, Double)] = binnedResults.rdd.map(row => (DataTypes.toInt(row.toSeq.last),
      weightColumnIndex match {
        case Some(i) => math.max(DataTypes.toDouble(row(i)), 0.0)
        case None => HistogramFunctions.UNWEIGHTED_OBSERVATION_SIZE
      })).reduceByKey(_ + _)

    val filledBins = pairedRDD.collect()
    val emptyBins = (0 to binnedResults.cutoffs.length - 2).map(i => (i, 0.0))
    //reduce by key and return either 0 or the value from filledBins
    val bins = (filledBins ++ emptyBins).groupBy(_._1).map {
      case (key, values) => (key, values.map(_._2).max)
    }.toList

    //sort by key return values
    val histSizes: Seq[Double] = bins.sortBy(_._1).map(_._2)

    val totalSize: Double = histSizes.sum
    val frequencies: Seq[Double] = histSizes.map(size => size / totalSize)

    Map("cutoffs" -> binnedResults.cutoffs, "hist" -> histSizes, "density" -> frequencies)
  }
}

object HistogramFunctions {
  val MAX_COMPUTED_NUMBER_OF_BINS: Int = 1000
  val UNWEIGHTED_OBSERVATION_SIZE: Double = 1.0

  def getNumBins(numBins: Option[Int], rdd: RDD[Row]): Int = {
    numBins match {
      case Some(n) => n
      case None =>
        math.min(math.floor(math.sqrt(rdd.count)), HistogramFunctions.MAX_COMPUTED_NUMBER_OF_BINS).toInt
    }
  }
}
