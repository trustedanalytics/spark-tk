package org.trustedanalytics.sparktk.frame.internal.ops

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.Row
import org.trustedanalytics.sparktk.frame.{ Column, DataTypes }
import org.trustedanalytics.sparktk.frame.internal.{ BaseFrame, FrameState, FrameTransform }
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd

trait BoxCoxTransform extends BaseFrame {

  def boxCox(columnName: String, lambdaValue: Double = 0d): Unit = {
    execute(BoxCox(columnName, lambdaValue))
  }
}

case class BoxCox(columnName: String, lambdaValue: Double) extends FrameTransform {

  require(columnName != null, "Column name cannot be null")

  override def work(state: FrameState): FrameState = {
    // run the operation
    val boxCoxRdd = BoxCox.boxCox(state, columnName, lambdaValue)

    // save results
    val updatedSchema = state.schema.addColumn(columnName + "_lambda_" + lambdaValue.toString, DataTypes.float64)

    FrameState(boxCoxRdd, updatedSchema)
  }
}

object BoxCox extends Serializable {
  /**
   * Computes the boxcox transform for each row of the frame
   *
   */
  def boxCox(frameRdd: FrameRdd, columnName: String, lambdaValue: Double): RDD[Row] = {
    frameRdd.mapRows(row => {
      val y = row.doubleValue(columnName)
      val boxCox = computeBoxCoxTransformation(y, lambdaValue)
      new GenericRow(row.valuesAsArray() :+ boxCox)
    })
  }

  def computeBoxCoxTransformation(y: Double, lambdaValue: Double): Double = {
    val boxCox: Double = if (lambdaValue == 0d) math.log(y) else (math.pow(y, lambdaValue) - 1) / lambdaValue
    boxCox
  }

}