package org.trustedanalytics.sparktk.frame.internal.ops.matrix

import org.apache.spark.sql.Row
import org.trustedanalytics.sparktk.frame.{DataTypes, Frame, Column}
import org.trustedanalytics.sparktk.frame.internal.{RowWrapper, FrameState, FrameTransform, BaseFrame}
import org.apache.spark.mllib.linalg.{ DenseMatrix => DM, Matrix, Matrices }
import breeze.linalg.{ DenseMatrix => BDM, Matrix => BM }

trait CovarianceMatrixTransform extends BaseFrame {
  
  def covarianceMatrix(matrixColumnName: String): Unit = {
    execute(CovarianceMatrix(matrixColumnName))
  }
}

case class CovarianceMatrix(matrixColumnName: String) extends FrameTransform {

  require(matrixColumnName != null, "Matrix column name cannot be null")

  override def work(state: FrameState): FrameState = {

    val frame = new Frame(state.rdd, state.schema)

    frame.schema.requireColumnIsType(matrixColumnName, DataTypes.matrix)
    //run the operation
    frame.addColumns(CovarianceMatrix.covarianceMatrix(matrixColumnName), Seq(Column("CovarianceMatrix", DataTypes.matrix)))
    FrameState(frame.rdd, frame.schema)
  }
}

object CovarianceMatrix extends Serializable {
  /**
   * Computes the covariance matrix for each matrix of the frame
   */

  def covarianceMatrix(matrixColumnName: String)(rowWrapper: RowWrapper): Row = {

    val matrix = rowWrapper.value(matrixColumnName).asInstanceOf[DM]
    val covarianceMatrix = m
  }
}