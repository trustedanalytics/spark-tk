package org.trustedanalytics.sparktk.frame.internal.ops.dotproduct

import org.trustedanalytics.sparktk.frame.{ DataTypes, Column }
import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameTransform, BaseFrame }
import org.trustedanalytics.sparktk.frame.internal.ops.cumulativedist.{ CumulativeDistFunctions, CumulativeSum }

/**
 * Created by kvadla on 4/25/16.
 */
trait DotProductTransform extends BaseFrame {

  def dotProduct(leftColumnNames: List[String],
                 rightColumnNames: List[String],
                 dotProductColumnName: String,
                 defaultLeftValues: Option[List[Double]] = None,
                 defaultRightValues: Option[List[Double]] = None): Unit = {
    execute(DotProduct(leftColumnNames, rightColumnNames, dotProductColumnName, defaultLeftValues, defaultRightValues))
  }
}

/**
 * Add column to frame with dot product.
 * @param leftColumnNames Names of columns used to create the left vector (A) for each row. Names should refer to a single column of type vector, or two or more columns of numeric scalars.
 * @param rightColumnNames Names of columns used to create right vector (B) for each row.Names should refer to a single column of type vector, or two or more columns of numeric scalars.
 * @param dotProductColumnName Name of column used to store the dot product.
 * @param defaultLeftValues Default values used to substitute null values in left vector.Default is None.
 * @param defaultRightValues Default values used to substitute null values in right vector.Default is None.
 */
case class DotProduct(leftColumnNames: List[String],
                      rightColumnNames: List[String],
                      dotProductColumnName: String,
                      defaultLeftValues: Option[List[Double]] = None,
                      defaultRightValues: Option[List[Double]] = None) extends FrameTransform {

  require(leftColumnNames.length != 0, "number of left columns cannot be zero")
  require(dotProductColumnName != null, "dot product column name cannot be null")

  override def work(state: FrameState): FrameState = {
    // run the operation
    val dotProductRdd = DotProductFunctions.dotProduct(state, leftColumnNames, rightColumnNames, defaultLeftValues, defaultRightValues)

    // save results
    val updatedSchema = state.schema.addColumn(dotProductColumnName, DataTypes.float64)

    FrameState(dotProductRdd, updatedSchema)
  }
}