package org.trustedanalytics.at.frame

import org.apache.spark.sql.Row
import org.trustedanalytics.at.interfaces._

trait TakeTrait extends BaseFrame {

  def take(n: Int): scala.Array[Row] = {
    execute(Take(n))
  }
}

/**
 * @param n - number of rows to take
 */
case class Take(n: Int) extends FrameSummarization[scala.Array[Row]] {

  override def work(immutableFrame: ImmutableFrame): scala.Array[Row] = {
    //immutableFrame.rdd.saveAsTextFile("/home/blbarker/tmp/scala_take")
    immutableFrame.rdd.take(n)
  }
}
