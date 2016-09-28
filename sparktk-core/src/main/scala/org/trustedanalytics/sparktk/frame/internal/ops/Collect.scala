package org.trustedanalytics.sparktk.frame.internal.ops

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameSummarization, BaseFrame }

trait CollectSummarization extends BaseFrame {
  /**
   * Collect all the frame data locally
   *
   * @param columns Name of columns; if specified, only data from these columns will be collected
   * @return Array of rows
   */
  def collect(columns: Option[Seq[String]] = None): scala.Array[Row] = {
    execute(Collect(columns))
  }
}

case class Collect(columns: Option[Seq[String]]) extends FrameSummarization[scala.Array[Row]] {

  override def work(state: FrameState): scala.Array[Row] = {
    columns match {
      case None => state.rdd.collect()
      case Some(cols) =>
        val indices = state.schema.columnIndices(cols)
        state.rdd.map(row => new GenericRow(indices.map(i => row(i)).toArray).asInstanceOf[Row]).collect()
    }
  }
}