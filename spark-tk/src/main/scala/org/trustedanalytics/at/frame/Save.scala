package org.trustedanalytics.at.frame

import org.apache.spark.frame.FrameRdd
import org.apache.spark.sql.DataFrame
import org.trustedanalytics.at.interfaces._

trait SaveTrait extends BaseFrame {

  def save(path: String): Unit = {
    execute(Save(path))
  }
}

case class Save(path: String) extends FrameSummarization[Unit] {

  override def work(immutableFrame: ImmutableFrame): Unit = {
    val frameRdd = new FrameRdd(immutableFrame.schema, immutableFrame.rdd)
    val df: DataFrame = frameRdd.toDataFrame
    df.write.parquet(path)
  }
}

