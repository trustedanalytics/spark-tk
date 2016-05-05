package org.trustedanalytics.sparktk.frame.internal.ops.join

import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.trustedanalytics.sparktk.frame.{ Frame, DataTypes }
import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameTransform, BaseFrame }

/**
 * Created by kvadla on 5/2/16.
 */
trait JoinTransform extends BaseFrame {

  def join(right: Frame,
           leftOn: List[String],
           rightOn: Option[List[String]] = None,
           how: String = "inner",
           name: Option[String] = None,
           skewedJoinType: Option[String] = None): Unit = {
    execute(Join(right, leftOn, rightOn, how, name, skewedJoinType))
  }
}

/***/
case class Join(right: Frame,
                leftOn: List[String],
                rightOn: Option[List[String]] = None,
                how: String = "inner",
                name: Option[String] = None,
                useBroadcast: Option[String] = None) extends FrameTransform {
  require(right != null, "right frame is required")
  require(leftOn != null, "left join column is required")
  require(rightOn != null, "right join column is required")
  require(how != null, "join method is required")
  require(useBroadcast.isEmpty
    || (useBroadcast.get == "left" || useBroadcast.get == "right"),
    "skewed join type should be 'left' or 'right'")

  override def work(state: FrameState): FrameState = {

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

    // Get estimated size of frame to determine whether to use a broadcast join
    //val broadcastJoinThreshold = "512MB"

    val joinedFrame = JoinRddFunctions.join(
      RddJoinParam(leftFrame, leftColumns),
      RddJoinParam(rightFrame, rightColumns),
      how,
      useBroadcast
    )

    //    val broadcastJoinThreshold = "512MB"
    //
    //    val joinedFrame = JoinRddFunctions.join(
    //      createRDDJoinParam(leftFrame, leftColumns, broadcastJoinThreshold),
    //      createRDDJoinParam(rightFrame, rightColumns, broadcastJoinThreshold),
    //      how,
    //      broadcastJoinThreshold,
    //      skewedJoinType
    //    )

    FrameState(joinedFrame, joinedFrame.schema)

    //    engine.frames.tryNewFrame(CreateEntityArgs(name = arguments.name,
    //      description = Some("created from join operation"))) {
    //      newFrame => newFrame.save(joinedFrame)
    //    }

  }

  //Create parameters for join
  //
  //  private def createRDDJoinParam(frame: FrameRdd,
  //                                 joinColumns: Seq[String],
  //                                 broadcastJoinThreshold: Long): RddJoinParam = {
  //    val frameSize = if (broadcastJoinThreshold > 0) frame.sizeInBytes else None
  //    val estimatedRddSize = frameSize match {
  //      case Some(size) => Some((size * 3).toLong)
  //      case _ => None
  //    }
  //    RddJoinParam(frame, joinColumns, estimatedRddSize)
  //  }
}
