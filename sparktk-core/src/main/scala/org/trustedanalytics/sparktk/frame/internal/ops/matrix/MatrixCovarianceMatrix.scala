/**
 *  Copyright (c) 2016 Intel Corporation 
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.trustedanalytics.sparktk.frame.internal.ops.matrix

import org.apache.spark.sql.Row
import org.trustedanalytics.sparktk.frame.{ DataTypes, Frame, Column }
import org.trustedanalytics.sparktk.frame.internal.{ RowWrapper, FrameState, FrameTransform, BaseFrame }
import org.apache.spark.mllib.linalg.{ DenseMatrix => DM, Matrix, Matrices }
import breeze.linalg.{ DenseMatrix => BDM, Matrix => BM, Axis, sum, DenseVector }

trait MatrixCovarianceMatrixTransform extends BaseFrame {

  def matrixCovarianceMatrix(matrixColumnName: String): Unit = {
    execute(MatrixCovarianceMatrix(matrixColumnName))
  }
}

case class MatrixCovarianceMatrix(matrixColumnName: String) extends FrameTransform {

  require(matrixColumnName != null, "Matrix column name cannot be null")

  override def work(state: FrameState): FrameState = {

    val frame = new Frame(state.rdd, state.schema)

    frame.schema.requireColumnIsType(matrixColumnName, DataTypes.matrix)
    //run the operation
    frame.addColumns(MatrixCovarianceMatrix.matrixCovarianceMatrix(matrixColumnName), Seq(Column("CovarianceMatrix_" + matrixColumnName, DataTypes.matrix)))
    FrameState(frame.rdd, frame.schema)
  }
}

object MatrixCovarianceMatrix extends Serializable {
  /**
   * Computes the covariance matrix for each matrix of the frame
   */

  def matrixCovarianceMatrix(matrixColumnName: String)(rowWrapper: RowWrapper): Row = {

    val matrix = rowWrapper.value(matrixColumnName).asInstanceOf[DM]
    val covarianceMatrix = computeCovarianceMatrix(MatrixFunctions.asBreeze(matrix))
    Row.apply(MatrixFunctions.fromBreeze(covarianceMatrix).asInstanceOf[DM])
  }

  /**
   * Computing the Covariance Matrix for a Breeze DenseMatrix
   * @param matrix Matrix whose Covariance Matrix is to be computed
   * @return Breeze DenseMatrix storing the covariance matrix
   */
  private def computeCovarianceMatrix(matrix: BDM[Double]): BDM[Double] = {
    val n = matrix.cols
    val denseMatrix: BDM[Double] = matrix.copy
    val mu: DenseVector[Double] = sum(denseMatrix, Axis._1) :* (1.0 / n)
    (0 until n).map(i => denseMatrix(::, i) :-= mu)
    val covarianceMatrix: BDM[Double] = (denseMatrix * denseMatrix.t) :* (1.0 / (n - 1))

    (covarianceMatrix + covarianceMatrix.t) :* (0.5)
  }
}