package org.trustedanalytics.sparktk.frame.internal.ops.matrix

import org.apache.spark.sql.Row
import org.trustedanalytics.sparktk.frame.{ Column, DataTypes, Frame }
import org.trustedanalytics.sparktk.frame.internal.{ RowWrapper, FrameState, FrameTransform, BaseFrame }
import org.apache.spark.mllib.linalg.{ DenseMatrix => DM, Matrix, Matrices }
import breeze.linalg.{ DenseMatrix => BDM, Matrix => BM }

trait PCATransform extends BaseFrame {

  def pca(matrixColumnName: String, vMatrixColumnName: String): Unit = {
    execute(PCA(matrixColumnName, vMatrixColumnName))
  }
}

case class PCA(matrixColumnName: String, vMatrixColumnName: String) extends FrameTransform {

  require(matrixColumnName != null, "Matrix column mame cannot be null")
  require(vMatrixColumnName != null, "VMatrix column name cannot be null")

  override def work(state: FrameState): FrameState = {

    val frame = new Frame(state.rdd, state.schema)

    frame.schema.requireColumnIsType(matrixColumnName, DataTypes.matrix)
    frame.schema.requireColumnIsType(vMatrixColumnName, DataTypes.matrix)

    frame.addColumns(PCA.pca(matrixColumnName, vMatrixColumnName), Seq(Column("PrincipalComponents", DataTypes.matrix)))
    FrameState(frame.rdd, frame.schema)
  }

}

object PCA extends Serializable {
  /*
  Computes the pca for each matrix of the frame using the U matrix
   */

  def pca(matrixColumn: String, vMatrixColumnName: String)(rowWrapper: RowWrapper): Row = {

    val matrix = rowWrapper.value(matrixColumn).asInstanceOf[DM]
    val breezeMatrix = SVD.asBreeze(matrix)

    val uMatrix = rowWrapper.value(vMatrixColumnName).asInstanceOf[DM]
    val breezeUMatrix = SVD.asBreeze(uMatrix)

    val pca = breezeMatrix :* breezeUMatrix
    val pcaMatrix = SVD.fromBreeze(pca).asInstanceOf[DM]

    Row.apply(pcaMatrix)
  }
}