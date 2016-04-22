package org.trustedanalytics.sparktk.frame.internal.ops.statistics.descriptives

import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import scala.math.Ordering._

/**
 * Compute categorical summary for an RDD with a single column
 */
object CategoricalSummaryFunctions {

  /**
   * Compute Categorical Summary for a FrameRdd with a single column
   * @param rdd FrameRdd containing data and schema information for a single column
   * @param rowCount Number of rows in the FrameRdd
   * @param topK User input to display top k most occurring items in the column
   * @param threshold User input to display all categories which appear more than the threshold percentage
   * @param default_top_k Default top k value from plugin configuration
   * @param default_threshold Default threshold value from plugin configuration
   * @return CategoricalSummaryOutput consisting of (Category, Frequency, Percentage) for a single column
   */
  def getSummaryStatistics(rdd: FrameRdd,
                           rowCount: Double,
                           topK: Option[Int],
                           threshold: Option[Double],
                           default_top_k: Int,
                           default_threshold: Double): CategoricalSummaryOutput = {

    val mappedRdd = rdd.mapRows(elem => (elem.values().head, 1)).persist(StorageLevel.MEMORY_AND_DISK)
    val filteredRdd = mappedRdd.filter(!matchMissingValues(_))
      .map { case (s, c) => (s.toString, c) }
      .reduceByKey(_ + _)
      .map { case (level: Any, count: Int) => (level.toString, count) }
    implicit val count = rowCount

    // Pruning logic here
    val res: Array[(Int, Double, String)] = (topK.getOrElse(None), threshold.getOrElse(None)) match {
      case (tk: Int, None) => pruneRddForTopK(filteredRdd, tk)
      case (None, th: Double) => pruneRddWithThreshold(filteredRdd, th)
      case (tk: Int, th: Double) => pruneRddWithTopKAndThreshold(filteredRdd, tk, th)
      case _ => pruneRddWithTopKAndThreshold(filteredRdd, default_top_k, default_threshold)
    }

    val categoricalSummaryLevels = res.map(elem => (elem._3, elem._1, elem._2)).map(elem => LevelData(elem._1, elem._2, elem._3)).toList

    val missingCategoryLevel = getMissingCategoryLevel(mappedRdd)

    val otherCategoryLevel = getOtherCategoryLevel(categoricalSummaryLevels, missingCategoryLevel.frequency)

    val finalResultWithAdditionalLevels = categoricalSummaryLevels :+ missingCategoryLevel :+ otherCategoryLevel

    mappedRdd.unpersist()

    CategoricalSummaryOutput(rdd.frameSchema.columnNames.head, finalResultWithAdditionalLevels)
  }

  /**
   * Check if an elem has missing value ("" or null) and return true; false otherwise
   */
  def matchMissingValues(elem: (Any, Int)): Boolean = elem._1 match {
    case level if level == null || level.toString == "" => true
    case _ => false
  }

  // Prune the RDD of Grouped values with frequency based on topk
  def pruneRddForTopK(filteredRdd: RDD[(String, Int)], topK: Int)(implicit rowCount: Double) =
    filteredRdd.map(_.swap).top(topK)
      .map { case (cnt, data) => (cnt, cnt / rowCount, data) }

  // Prune the RDD of Grouped values with frequency based on threshold
  // For each item to be returned, frequency/rowCount > threshold
  def pruneRddWithThreshold(filteredRdd: RDD[(String, Int)], threshold: Double)(implicit rowCount: Double) = {
    filteredRdd.map(_.swap)
      .map { case (cnt, data) => (cnt, cnt / rowCount, data) }
      .filter(elem => elem._2 >= threshold).collect()
  }

  /**
   * Prune RDD and return the levels which satisfy given topk and threshold
   * @param filteredRdd RDD grouped by value (String) and frequency (Int)
   * @param topK top k items to fetch from the rdd
   * @param threshold items to fetch from rdd which satisfy the threshold.
   *                  Matching items should have frequency/rowCount > threshold
   * @param rowCount  Number of items in RDD
   * @return Array[(Int, Double, String)] representing Array of Tuple(Frequency, Percentage, Value)
   */
  def pruneRddWithTopKAndThreshold(filteredRdd: RDD[(String, Int)], topK: Int, threshold: Double)(implicit rowCount: Double) = {
    filteredRdd.map(_.swap).top(topK)
      .map { case (cnt, data) => (cnt, cnt / rowCount, data) }
      .filter(elem => elem._2 >= threshold)
  }

  // Get the "Missing" Category Level summary
  def getMissingCategoryLevel(rdd: RDD[(Any, Int)])(implicit rowCount: Double): LevelData = {
    val missingValuesCount = rdd.filter(matchMissingValues(_))
      .map(_._2).fold(0)(_ + _)
    LevelData("Missing", missingValuesCount, missingValuesCount / rowCount)
  }

  // Get the total count for all summary levels which satisfy the user criterion
  def getTotalCountForSummaryLevels(categoricalSummaryLevels: List[LevelData]): Int =
    categoricalSummaryLevels.map(elem => elem.frequency).sum

  // Get the "Other" Category Level summary
  def getOtherCategoryLevel(categoricalSummaryLevels: List[LevelData], missingValueCount: Int)(implicit rowCount: Double) = {
    val otherCategoryCount = rowCount - getTotalCountForSummaryLevels(categoricalSummaryLevels) - missingValueCount
    LevelData("Other", otherCategoryCount.toInt, otherCategoryCount / rowCount)
  }
}
