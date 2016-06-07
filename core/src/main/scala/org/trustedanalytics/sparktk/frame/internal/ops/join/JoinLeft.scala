package org.trustedanalytics.sparktk.frame.internal.ops.join

import org.trustedanalytics.sparktk.frame.{ SchemaHelper, DataTypes, Frame }
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameSummarization, BaseFrame }

trait JoinLeftSummarization extends BaseFrame {

  /**
    * Join operation on one or two frames, creating a new frame.
    *
    * @param right Another frame to join with.
    * @param leftOn Names of the columns in the left frame used to match up the two frames.
    * @param rightOn Names of the columns in the right frame used to match up the two frames. Default is the same as the left frame.
    * @param useBroadcastRight If right table is small enough to fit in the memory of a single machine, you can set useBroadcastRight to True to perform broadcast join.
    * Default is False.
    */
  def joinLeft(right: Frame,
               leftOn: List[String],
               rightOn: Option[List[String]] = None,
               useBroadcastRight: Boolean = false): Frame = {
    execute(JoinLeft(right, leftOn, rightOn, useBroadcastRight))
  }
}

case class JoinLeft(right: Frame,
                    leftOn: List[String],
                    rightOn: Option[List[String]],
                    useBroadcastRight: Boolean) extends FrameSummarization[Frame] {

  require(right != null, "right frame is required")
  require(leftOn != null || leftOn.nonEmpty, "left join column is required")
  require(rightOn != null, "right join column is required")

  override def work(state: FrameState): Frame = {

    val leftFrame: FrameRdd = state
    val rightFrame: FrameRdd = new FrameRdd(right.schema, right.rdd)

    //first validate join columns are valid
    val leftColumns = leftOn
    val rightColumns = rightOn.getOrElse(leftOn)

    //First validates join columns are valid and checks left join column is compatible with right join columns
    SchemaHelper.checkValidColumnsExistAndCompatible(leftFrame, rightFrame, leftColumns, rightColumns)

    val joinedFrame = JoinRddFunctions.leftJoin(
      RddJoinParam(leftFrame, leftColumns),
      RddJoinParam(rightFrame, rightColumns),
      useBroadcastRight
    )
    new Frame(joinedFrame, joinedFrame.schema)
  }
}
