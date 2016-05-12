package org.trustedanalytics.sparktk.frame.internal.ops

import org.apache.spark.sql.DataFrame
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameSummarization, BaseFrame }
import org.trustedanalytics.sparktk.saveload.TkSaveLoad

trait SaveSummarization extends BaseFrame {

  def save(path: String): Unit = {
    execute(Save(path))
  }
}

case class Save(path: String) extends FrameSummarization[Unit] {

  override def work(state: FrameState): Unit = {
    val frameRdd = new FrameRdd(state.schema, state.rdd)
    val df: DataFrame = frameRdd.toDataFrame
    df.write.parquet(path)
    val formatId = "Frame"
    val formatVersion = 1
    TkSaveLoad.saveTk(state.rdd.sparkContext, path, formatId, formatVersion, "No Metadata")
  }
}

