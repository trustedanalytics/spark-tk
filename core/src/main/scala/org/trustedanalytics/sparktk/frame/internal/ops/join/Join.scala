package org.trustedanalytics.sparktk.frame.internal.ops.join

import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.trustedanalytics.sparktk.frame.{ Frame, DataTypes }
import org.trustedanalytics.sparktk.frame.internal.{ BaseFrame, FrameSummarization, FrameState }

trait JoinSummarization extends BaseFrame {

  def join(right: Frame,
           leftOn: List[String],
           rightOn: Option[List[String]] = None,
           how: String = "inner",
           name: Option[String] = None,
           useBroadcast: Option[String] = None): Frame = {
    execute(Join(right, leftOn, rightOn, how, name, useBroadcast))
  }
}

/**
 * Join operation on one or two frames, creating a new frame.
 *
 * @param right Another frame to join with.
 * @param leftOn Names of the columns in the left frame used to match up the two frames.
 * @param rightOn Names of the columns in the right frame used to match up the two frames. Default is the same as the left frame.
 * @param how How to qualify the data to be joined together. Must be one of the following: (left, right, inner or outer).Default is inner
 * @param name Name of the result grouped frame
 * @param useBroadcast If your tables are smaller broadcast is used for join. Specify which table is smaller(left or right). Default is None
 */
case class Join(right: Frame,
                leftOn: List[String],
                rightOn: Option[List[String]] = None,
                how: String = "inner",
                name: Option[String] = None,
                useBroadcast: Option[String] = None) extends FrameSummarization[Frame] {
  require(right != null, "right frame is required")
  require(leftOn != null || leftOn.length !=0, "left join column is required")
  require(rightOn != null, "right join column is required")
  require(how != null, "join method is required")
  require(useBroadcast.isEmpty
    || (useBroadcast.get == "left" || useBroadcast.get == "right"),
    "useBroadcast join type should be 'left' or 'right'")

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

    val joinedFrame = JoinRddFunctions.join(
      RddJoinParam(leftFrame, leftColumns),
      RddJoinParam(rightFrame, rightColumns),
      how,
      useBroadcast
    )
    new Frame(joinedFrame, joinedFrame.schema)
  }
}
