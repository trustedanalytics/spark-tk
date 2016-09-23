package org.trustedanalytics.sparktk.frame.internal.ops.matrix

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.Row
import org.apache.spark.mllib.linalg.{ DenseMatrix => DM, Matrix, Matrices }
import breeze.linalg.{ DenseMatrix => BDM, Matrix => BM }
import org.trustedanalytics.sparktk.frame.{ Frame, Column, DataTypes }
import org.trustedanalytics.sparktk.frame.internal._
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd

trait SVDTransform extends BaseFrame {

  def svd(matrixColumnName: String): Unit = {
    execute(SVD(matrixColumnName))
  }
}

case class SVD(matrixColumnName: String) extends FrameTransform {

  require(matrixColumnName != null, "Matrix column name cannot be null")

  override def work(state: FrameState): FrameState = {

    val frame = new Frame(state.rdd, state.schema)

    frame.schema.requireColumnIsType(matrixColumnName, DataTypes.matrix)
    //run the operation
    frame.addColumns(SVD.svd(matrixColumnName), Seq(Column("U", DataTypes.matrix),
      Column("Vt", DataTypes.matrix),
      Column("SingularVectors", DataTypes.matrix)))
    FrameState(frame.rdd, frame.schema)

  }
}

object SVD extends Serializable {
  /**
   * Computes the svd for each matrix of the frame
   */
  def svd(matrixColumnName: String)(rowWrapper: RowWrapper): Row = {
    
    val matrix = rowWrapper.value(matrixColumnName).asInstanceOf[DM]
    val breezeMatrix = asBreeze(matrix)

    val svdResult = breeze.linalg.svd(breezeMatrix)

    val uMatrix = fromBreeze(svdResult.Vt).asInstanceOf[DM]
    val vMatrix = fromBreeze(svdResult.U).asInstanceOf[DM]
    val singularVectors = new DM(1, svdResult.singularValues.length, svdResult.singularValues.toArray, false)

    Row.apply(uMatrix, vMatrix, singularVectors)
  }

  def fromBreeze(breezeDM: BDM[Double]): Matrix = {
    new DM(breezeDM.rows, breezeDM.cols, breezeDM.data, breezeDM.isTranspose)

  }

  def asBreeze(mllibDM: DM): BDM[Double] = {
    if (!mllibDM.isTransposed) {
      new BDM[Double](mllibDM.numRows, mllibDM.numCols, mllibDM.values)
    }
    else {
      val breezeMatrix = new BDM[Double](mllibDM.numCols, mllibDM.numRows, mllibDM.values)
      breezeMatrix.t
    }
  }

}