package org.trustedanalytics.sparktk.frame.internal.ops.statistics

/**
 * Library for creating, cleaning and processing weighted data.
 */
object NumericValidationUtils extends Serializable {

  /**
   * True iff a double is a finite number.
   * @param double A Double.
   * @return True iff the double is a finite number..
   */
  def isFiniteNumber(double: Double) = { !double.isNaN && !double.isInfinite }

  /**
   * True iff the double is a finite number > 0.
   * @param x A double.
   * @return
   */
  def isFinitePositive(x: Double) = isFiniteNumber(x) && (x > 0)

  /**
   * True if both the data and the weight are finite numbers.
   * @param pair A pair of doubles.
   * @return
   */
  def isFiniteDoublePair(pair: (Double, Double)) = {

    isFiniteNumber(pair._1) && isFiniteNumber(pair._2)
  }
}
