package org.trustedanalytics.at.frame

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import scala.collection.JavaConverters._
import org.trustedanalytics.at.discrete.DiscretizationFunctions
import org.trustedanalytics.at.interfaces._
import org.trustedanalytics.at.schema.Column
import java.util.{ ArrayList => JArrayList }

trait BinColumnTrait extends BaseFrame {

  def binColumn(column: String,
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
case class BinColumn(column: String, //column: Column,
                     cutoffs: List[Double],
                     includeLowest: Boolean,
                     strictBinning: Boolean,
                     binColumnName: Option[String]) extends FrameTransform {

  override def work(immutableFrame: ImmutableFrame): ImmutableFrame = {
    val columnIndex = immutableFrame.schema.columnIndex(column)
    immutableFrame.schema.requireColumnIsNumerical(column)
    val newColumnName = binColumnName.getOrElse(immutableFrame.schema.getNewColumnName(column + "_binned"))
    val binnedRdd = DiscretizationFunctions.binColumns(columnIndex, cutoffs, includeLowest, strictBinning, immutableFrame.rdd)
    //binnedRdd.saveAsTextFile("/home/blbarker/tmp/binned")
    ImmutableFrame(binnedRdd, immutableFrame.schema.copy(columns = immutableFrame.schema.columns :+ Column(newColumnName, "int32")))
  }

}

