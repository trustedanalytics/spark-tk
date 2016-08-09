package org.trustedanalytics.sparktk.frame.internal.ops.statistics.correlation

import breeze.numerics._
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.apache.spark.mllib.linalg.{ Matrix }
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow

/**
 * Object for calculating correlation and the correlation matrix
 */

object CorrelationFunctions extends Serializable {

  /**
   * Compute correlation for exactly two columns
   *
   * @param frameRdd input rdd containing all columns
   * @param dataColumnNames column names for which we calculate the correlation
   * @return correlation wrapped in DoubleValue
   */
  def correlation(frameRdd: FrameRdd,
                  dataColumnNames: List[String]): Double = {
    // compute correlation

    val correlation: Matrix = Statistics.corr(frameRdd.toDenseVectorRdd(dataColumnNames))

    val dblVal: Double = correlation.toArray(1)

    if (dblVal.isNaN || abs(dblVal) < .000001) 0 else dblVal
  }

  /**
   * Compute correlation for two or more columns
   *
   * @param frameRdd input rdd containing all columns
   * @param dataColumnNames column names for which we calculate the correlation matrix
   * @return the correlation matrix in a RDD[Rows]
   */
  def correlationMatrix(frameRdd: FrameRdd,
                        dataColumnNames: List[String]): RDD[Row] = {

    val correlation: Matrix = Statistics.corr(frameRdd.toDenseVectorRdd(dataColumnNames))
    val vecArray = correlation.toArray.grouped(correlation.numCols).toArray
    val arrGenericRow = vecArray.map(row => {
      val temp: Array[Any] = row.map(x => if (x.isNaN || abs(x) < .000001) 0 else x)
      new GenericRow(temp)
    })

    frameRdd.sparkContext.parallelize(arrGenericRow)
  }
}
