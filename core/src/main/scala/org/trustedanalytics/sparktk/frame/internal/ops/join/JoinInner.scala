package org.trustedanalytics.sparktk.frame.internal.ops.join

import org.trustedanalytics.sparktk.frame.{ DataTypes, Frame }
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameSummarization, BaseFrame }

trait JoinInnerSummarization extends BaseFrame {
  def joinInner(right: Frame,
                leftOn: List[String],
                rightOn: Option[List[String]] = None,
                useBroadcast: Option[String] = None): Frame = {
    execute(JoinInner(right, leftOn, rightOn, useBroadcast))
  }
}

/**
 * JoinInner performs left join operation on one or two frames, creating a new frame.
 *
 * @param right        Another frame to join with.
 * @param leftOn       Names of the columns in the left frame used to match up the two frames.
 * @param rightOn      Names of the columns in the right frame used to match up the two frames. Default is the same as the left frame.
 * @param useBroadcast If one of your tables is small enough to fit in the memory of a single machine, you can use a broadcast join.
 *                     Specify which table to broadcast (left or right). Default is None.
 */
case class JoinInner(right: Frame,
                     leftOn: List[String],
                     rightOn: Option[List[String]],
                     useBroadcast: Option[String]) extends FrameSummarization[Frame] {

  require(right != null, "right frame is required")
  require(leftOn != null || leftOn.length != 0, "left join column is required")
  require(rightOn != null, "right join column is required")
  require(useBroadcast.isEmpty
    || (useBroadcast.get == "left" || useBroadcast.get == "right"),
    "useBroadcast join type should be 'left' or 'right'. Default is none")

  override def work(state: FrameState): Frame = {

    val leftFrame: FrameRdd = state
    val rightFrame: FrameRdd = new FrameRdd(right.schema, right.rdd)

    //first validate join columns are valid
    val leftColumns = leftOn
    val rightColumns = rightOn.getOrElse(leftOn)
    leftFrame.schema.validateColumnsExist(leftColumns)
    rightFrame.schema.validateColumnsExist(rightColumns)

    //Check left join column is compatiable with right join column
    (leftColumns zip rightColumns).map {
      case (leftJoinCol, rightJoinCol) => require(DataTypes.isCompatibleDataType(
        leftFrame.schema.columnDataType(leftJoinCol),
        rightFrame.schema.columnDataType(rightJoinCol)),
        "Join columns must have compatible data types")
    }

    val joinedFrame = JoinRddFunctions.innerJoin(
      RddJoinParam(leftFrame, leftColumns),
      RddJoinParam(rightFrame, rightColumns),
      useBroadcast
    )
    new Frame(joinedFrame, joinedFrame.schema)
  }
}
