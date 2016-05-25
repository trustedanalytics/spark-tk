package org.trustedanalytics.sparktk.frame.internal.ops

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.Row
import org.trustedanalytics.sparktk.frame.DataTypes
import org.trustedanalytics.sparktk.frame.internal.{ BaseFrame, FrameState, FrameTransform, VectorFunctions }
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd

trait DotProductTransform extends BaseFrame {
  /**
   * Add column to frame with dot product.
   *
   * Calculate the dot product for each row in a frame using values from two equal-length sequences of columns.
   *
   * Dot product is computed by the following formula:
   *
   * The dot product of two vectors :math:`A=[a_1, a_2, ..., a_n]` and :math:`B =[b_1, b_2, ..., b_n]` is
   * :math:`a_1*b_1 + a_2*b_2 + ...+ a_n*b_n`.  The dot product for each row is stored in a new column in the
   * existing frame.
   *
   * @note If default_left_values or default_right_values are not specified, any null values will be replaced by zeros.
   *
   * @param leftColumnNames Names of columns used to create the left vector (A) for each row. Names should refer to a
   *                        single column of type vector, or two or more columns of numeric scalars.
   * @param rightColumnNames Names of columns used to create right vector (B) for each row.Names should refer to a
   *                         single column of type vector, or two or more columns of numeric scalars.
   * @param dotProductColumnName Name of column used to store the dot product.
   * @param defaultLeftValues Default values used to substitute null values in left vector.Default is None.
   * @param defaultRightValues Default values used to substitute null values in right vector.Default is None.
   */
  def dotProduct(leftColumnNames: List[String],
                 rightColumnNames: List[String],
                 dotProductColumnName: String,
                 defaultLeftValues: Option[List[Double]] = None,
                 defaultRightValues: Option[List[Double]] = None): Unit = {
    execute(DotProduct(leftColumnNames, rightColumnNames, dotProductColumnName, defaultLeftValues, defaultRightValues))
  }
}

case class DotProduct(leftColumnNames: List[String],
                      rightColumnNames: List[String],
                      dotProductColumnName: String,
                      defaultLeftValues: Option[List[Double]] = None,
                      defaultRightValues: Option[List[Double]] = None) extends FrameTransform {

  require(leftColumnNames.length != 0, "number of left columns cannot be zero")
  require(dotProductColumnName != null, "dot product column name cannot be null")

  override def work(state: FrameState): FrameState = {
    // run the operation
    val dotProductRdd = DotProduct.dotProduct(state, leftColumnNames, rightColumnNames, defaultLeftValues, defaultRightValues)

    // save results
    val updatedSchema = state.schema.addColumn(dotProductColumnName, DataTypes.float64)

    FrameState(dotProductRdd, updatedSchema)
  }
}

object DotProduct extends Serializable {
  /**
   * Computes the dot product for each row in the frame.
   *
   * Uses the left column values, and the right column values to compute the dot product for each row.
   *
   * @param frameRdd Data frame
   * @param leftColumnNames Left column names
   * @param rightColumnNames Right column names
   * @param defaultLeftValues  Optional default values used to substitute null values in the left columns
   * @param defaultRightValues Optional default values used to substitute null values in the right columns
   * @return Data frame with an additional column containing the dot product
   */
  def dotProduct(frameRdd: FrameRdd, leftColumnNames: List[String], rightColumnNames: List[String],
                 defaultLeftValues: Option[List[Double]] = None,
                 defaultRightValues: Option[List[Double]] = None): RDD[Row] = {

    frameRdd.frameSchema.requireColumnsAreVectorizable(leftColumnNames)
    frameRdd.frameSchema.requireColumnsAreVectorizable(rightColumnNames)

    val leftVectorSize = getVectorSize(frameRdd, leftColumnNames)
    val rightVectorSize = getVectorSize(frameRdd, rightColumnNames)

    require(leftVectorSize == rightVectorSize,
      "number of elements in left columns should equal number in right columns")
    require(defaultLeftValues.isEmpty || defaultLeftValues.get.size == leftVectorSize,
      "size of default left values should match number of elements in left columns")
    require(defaultRightValues.isEmpty || defaultRightValues.get.size == leftVectorSize,
      "size of default right values should match number of elements in right columns")

    frameRdd.mapRows(row => {
      val leftVector = VectorFunctions.createVector(row, leftColumnNames, defaultLeftValues)
      val rightVector = VectorFunctions.createVector(row, rightColumnNames, defaultRightValues)
      val dotProduct = VectorFunctions.dotProduct(leftVector, rightVector)
      new GenericRow(row.valuesAsArray() :+ dotProduct)
    })
  }

  private def getVectorSize(frameRdd: FrameRdd, columnNames: List[String]): Int = {
    val frameSchema = frameRdd.frameSchema
    columnNames.map(name =>
      frameSchema.columnDataType(name) match {
        case DataTypes.vector(length) => length
        case _ => 1
      }).sum.toInt
  }
}