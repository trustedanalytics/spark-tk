package org.trustedanalytics.sparktk.frame.internal.ops.sample

import org.apache.spark.rdd._

import scala.reflect.ClassTag
import scala.util.Random

/**
 * Class that represents the entry content and label of a data point.
 *
 * @param label for this data point.
 * @param entry content for this data point.
 */
case class LabeledLine[L: ClassTag, T: ClassTag](label: L, entry: T)

/**
 * Data Splitter for ML algorithms. It randomly labels an input RDD with user
 * specified percentage for each category.
 *
 * TODO: this class doesn't really belong in the Engine but it is shared code that both frame-plugins and graph-plugins need access to
 *
 * @param percentages A double array stores percentages.
 * @param seed Random seed for random number generator.
 */
class MLDataSplitter(percentages: Array[Double], labels: Array[String], seed: Int) extends Serializable {

  require(percentages.forall(p => p > 0d), "MLDataSplitter: Some percentage numbers are negative or zero.")
  require(Math.abs(percentages.sum - 1.0d) < 0.000000001d, "MLDataSplitter: Sum of percentages does not equal  1.")
  require(labels.length == percentages.length, "Number of class labels differs from number of percentages given.")

  var cdf: Array[Double] = percentages.scanLeft(0.0d)(_ + _)
  cdf = cdf.drop(1)

  // clamp the final value to 1.0d so that we cannot get rare (but in big data, still possible!)
  // occurrences where the sample value falls between the gap of the summed input probabilities and 1.0d
  cdf(cdf.length - 1) = 1.0d

  /**
   * Randomly label each entry of an input RDD according to user specified percentage
   * for each category.
   *
   * @param inputRDD RDD of type T.
   */
  def randomlyLabelRDD[T: ClassTag](inputRDD: RDD[T]): RDD[LabeledLine[String, T]] = {
    // generate auxiliary (sample) RDD
    val auxiliaryRDD: RDD[(T, Double)] = inputRDD.mapPartitionsWithIndex({ case (i, p) => addRandomValues(seed, i, p) })

    val labeledRDD = auxiliaryRDD.map { p =>
      val (line, sampleValue) = p
      val label = labels.apply(cdf.indexWhere(_ >= sampleValue))
      LabeledLine(label, line)
    }
    labeledRDD
  }

  private def addRandomValues[T: ClassTag](seed: Int, index: Int, it: Iterator[T]): Iterator[(T, Double)] = {
    val pseudoRandomGenerator = new Random(seed + index)
    it.map(x => (x, pseudoRandomGenerator.nextDouble()))
  }
}
