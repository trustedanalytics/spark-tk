package org.trustedanalytics.sparktk.frame.internal.ops

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameSummarization, BaseFrame }

import scala.collection.mutable.ListBuffer

trait TakeSummarization extends BaseFrame {
  /**
   * Get data subset.
   *
   * @param n Number of rows to take
   * @param offset Optional offset into the frame where the take will start
   * @param columns Name of columns; if specified, only data from these columns will be collected
   * @return Array of rows
   */
  def take(n: Int, offset: Int = 0, columns: Option[Seq[String]] = None): scala.Array[Row] = {
    execute(Take(n, offset, columns))
  }

  //def takePython(n: Int): scala.Array[Any]Scala.Array[Byte]]
}

case class Take(n: Int, offset: Int, columns: Option[Seq[String]]) extends FrameSummarization[scala.Array[Row]] {
  require(offset >= 0, s"offset must be greater than or equal to 0, received $offset")

  override def work(state: FrameState): scala.Array[Row] = {
    if (offset == 0) {
      columns match {
        case None => state.rdd.take(n)
        case Some(cols) =>
          val indices = state.schema.columnIndices(cols)
          state.rdd.map(row => new GenericRow(indices.map(i => row(i)).toArray).asInstanceOf[Row]).take(n)
      }
    }
    else {
      // have an offset, so deal slowly with an iterator

      val it = state.rdd.toLocalIterator
      val array = new Array[Row](n)

      val indices = columns match {
        case None => null
        case Some(cols) => state.schema.columnIndices(cols)
      }

      var row_index = 0

      // skip records
      while (row_index < offset && it.hasNext) {
        row_index += 1
        it.next()
      }
      // collect record
      row_index = 0
      while (row_index < n && it.hasNext) {
        val row = it.next()
        array(row_index) = if (indices == null) { row } else { new GenericRow(indices.map(i => row(i)).toArray).asInstanceOf[Row] }
        row_index += 1
      }

      // check if we need to truncate
      if (row_index < n) { array.slice(0, n) } else array
    }
  }
}
