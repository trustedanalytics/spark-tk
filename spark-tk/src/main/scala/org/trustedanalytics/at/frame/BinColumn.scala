package org.trustedanalytics.at.frame

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.trustedanalytics.at.discrete.DiscretizationFunctions
import org.trustedanalytics.at.interfaces._
import org.trustedanalytics.at.schema.Column

trait BinColumnTrait extends BaseFrame {

  def binColumn(column: Column,
                cutoffs: List[Double],
                includeLowest: Boolean = true,
                strictBinning: Boolean = false,
                binColumnName: Option[String] = None): Unit = {

    execute(BinColumn(column, cutoffs, includeLowest, strictBinning, binColumnName))
  }
}

/**
 *
 * @param column the column to bin
 * @param cutoffs list of bin cutoff points, which must be progressively increasing; all bin boundaries must be
 *                defined, so with N bins, N+1 values are required
 * @param includeLowest true means the lower bound is inclusive, where false means the upper bound is inclusive
 * @param strictBinning if true, each value less than the first cutoff value or greater than the last cutoff value
 *                      will be assigned to a bin value of -1; if false, values less than the first cutoff value will
 *                      be placed in the first bin, and those beyond the last cutoff will go in the last bin
 * @param binColumnName The name of the new column may be optionally specified
 */
case class BinColumn(column: Column,
                     cutoffs: List[Double],
                     includeLowest: Boolean,
                     strictBinning: Boolean,
                     binColumnName: Option[String]) extends FrameTransform {

  override def work(immutableFrame: ImmutableFrame): ImmutableFrame = {
    // get column index
    val columnIndex = 0
    //    val frame: SparkFrame = arguments.frame
    //    val columnIndex = frame.schema.columnIndex(arguments.columnName)
    //    frame.schema.requireColumnIsNumerical(arguments.columnName)

    //   val binColumnName = arguments.binColumnName.getOrElse(frame.schema.getNewColumnName(arguments.columnName + "_binned"))

    // run the operation and save results
    //val updatedSchema = frame.schema.addColumn(binColumnName, DataTypes.int32)
    //val binnedRdd = DiscretizationFunctions.binColumns(columnIndex, cutoffs, includeLowest, strictBinning, null)
    val rdd: RDD[Row] = LoadRddFunctions.toRowRdd(immutableFrame.rdd, immutableFrame.schema)
    val binnedRdd = DiscretizationFunctions.binColumns(columnIndex, cutoffs, includeLowest, strictBinning, rdd)

    ImmutableFrame(binnedRdd, immutableFrame.schema.copy(columns = immutableFrame.schema.columns :+ Column("binned", "int32")))
    //ImmutableFrame(null, immutableFrame.schema.copy(columns = immutableFrame.schema.columns :+ Column("binned", "int32")))
    //frame.save(new FrameRdd(updatedSchema, binnedRdd))
  }

}

