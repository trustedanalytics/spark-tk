package org.trustedanalytics.sparktk.frame.internal.ops.zip

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.trustedanalytics.sparktk.frame.Column
import org.trustedanalytics.sparktk.frame.internal.{ BaseFrame, FrameState, FrameTransform }

trait ZipWithIndexedRddTransform extends BaseFrame {
  def zipWithIndexedRdd(rdd: RDD[(Long, Row)],
                        newColumns: Seq[Column]): Unit = {
    execute(ZipWithIndexedRdd(rdd, newColumns))
  }

}

case class ZipWithIndexedRdd(rdd: RDD[(Long, Row)],
                             newColumns: Seq[Column]) extends FrameTransform {

  override def work(state: FrameState): FrameState = {

    val indexedFrameRdd = state.rdd.zipWithIndex().map { case (row, index) => (index, row) }

    val x = rdd.join(indexedFrameRdd).collect()

    val resultRdd: RDD[Row] = rdd.join(indexedFrameRdd).map { value =>
      val row = value._2._2
      val cluster = value._2._1
      Row.merge(row, cluster)
    }

    FrameState(resultRdd, state.schema.copy(columns = state.schema.columns ++ newColumns))
  }
}