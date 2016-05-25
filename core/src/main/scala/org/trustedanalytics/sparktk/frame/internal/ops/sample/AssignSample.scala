package org.trustedanalytics.sparktk.frame.internal.ops.sample

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.trustedanalytics.sparktk.frame._
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.trustedanalytics.sparktk.frame.internal.{ BaseFrame, FrameState, FrameTransform }

trait AssignSampleTransform extends BaseFrame {
  /**
   * Randomly group rows into user-defined classes.
   *
   * Randomly assign classes to rows given a vector of percentages. The table receives an additional column that
   * contains a random label. The random label is generated by a probability distribution function. The distribution
   * function is specified by the sample_percentages, a list of floating point values, which add up to 1. The labels
   * are non-negative integers drawn from the range :math:`[ 0, len(S) - 1]` where :math:`S` is the
   * '''samplePercentages'''.
   *
   * @note The sample percentages provided by the user are preserved to at least eight decimal places, but beyond
   *       this there may be small changes due to floating point imprecision.
   *
   *       In particular:
   *
   *       1. The engine validates that the sum of probabilities sums to 1.0 within eight decimal places and returns
   *       an error if the sum falls outside of this range.
   *       1. The probability of the final class is clamped so that each row receives a valid label with probability
   *       one.
   *
   * @param samplePercentages Entries are non-negative and sum to 1. (See the note below.)
   *                          If the '''i'''th entry of the  list is '''p''', then then each row
   *                          receives label *i* with independent probability '''p'''.""")
   * @param sampleLabels Names to be used for the split classes. Defaults to "TR", "TE",
   *                     "VA" when the length of '''samplePercentages''' is 3, and defaults
   *                     to Sample_0, Sample_1, ... otherwise.
   * @param outputColumn Name of the new column which holds the labels generated by the
   *                     function
   * @param randomSeed Random seed used to generate the labels.  Defaults to 0.
   */
  def assignSample(samplePercentages: List[Double],
                   sampleLabels: Option[List[String]] = None,
                   outputColumn: Option[String] = None,
                   randomSeed: Option[Int] = None): Unit = {
    execute(AssignSample(samplePercentages, sampleLabels, outputColumn, randomSeed))
  }
}

case class AssignSample(samplePercentages: List[Double],
                        sampleLabels: Option[List[String]] = None,
                        outputColumn: Option[String] = None,
                        randomSeed: Option[Int] = None) extends FrameTransform {

  def splitLabels: Array[String] = if (sampleLabels.isEmpty) {
    if (samplePercentages.length == 3) {
      Array("TR", "TE", "VA")
    }
    else {
      samplePercentages.indices.map(i => "Sample_" + i).toArray
    }
  }
  else {
    sampleLabels.get.toArray
  }

  require(samplePercentages != null, "AssignSample requires that the percentages vector be non-null.")
  require(samplePercentages.nonEmpty, "AssignSample  requires that the percentages vector contain at least one value.")

  require(samplePercentages.forall(_ >= 0.0d), "AssignSample requires that all percentages be non-negative.")
  require(samplePercentages.forall(_ <= 1.0d), "AssignSample requires that all percentages be no more than 1.")

  lazy val sumOfPercentages = samplePercentages.sum

  require(sumOfPercentages > 1.0d - 0.000000001,
    "AssignSample:  Sum of provided probabilities falls below one (" + sumOfPercentages + ")")
  require(sumOfPercentages < 1.0d + 0.000000001,
    "AssignSample:  Sum of provided probabilities exceeds one (" + sumOfPercentages + ")")

  override def work(state: FrameState): FrameState = {
    def seed = randomSeed.getOrElse(0)
    def outputColumnName = outputColumn.getOrElse(state.schema.getNewColumnName("sample_bin"))

    // run the operation
    val splitter = new MLDataSplitter(samplePercentages.toArray, splitLabels, seed)
    val labeledRDD: RDD[LabeledLine[String, Row]] = splitter.randomlyLabelRDD(state.rdd)

    val splitRDD: RDD[Array[Any]] = labeledRDD.map((x: LabeledLine[String, Row]) =>
      (x.entry.toSeq :+ x.label.asInstanceOf[Any]).toArray[Any]
    )
    val updatedSchema = state.schema.addColumn(outputColumnName, DataTypes.string)
    FrameRdd.toFrameRdd(updatedSchema, splitRDD)

  }
}