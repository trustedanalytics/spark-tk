package org.trustedanalytics.at.frame.ops

import org.apache.spark.org.trustedanalytics.at.frame.FrameRdd
import org.apache.spark.sql.DataFrame
import org.trustedanalytics.at.frame.{ FrameState, FrameSummarization, BaseFrame }

trait SaveTrait extends BaseFrame {

  def save(path: String): Unit = {
    execute(Save(path))
  }
}

case class Save(path: String) extends FrameSummarization[Unit] {

  override def work(state: FrameState): Unit = {
    val frameRdd = new FrameRdd(state.schema, state.rdd)
    val df: DataFrame = frameRdd.toDataFrame
    df.write.parquet(path)
  }
}

