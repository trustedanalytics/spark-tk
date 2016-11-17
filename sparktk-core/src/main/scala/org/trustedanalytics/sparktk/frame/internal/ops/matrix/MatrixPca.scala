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
import org.trustedanalytics.sparktk.frame.{ Column, DataTypes, Frame }
import org.trustedanalytics.sparktk.frame.internal.{ RowWrapper, FrameState, FrameTransform, BaseFrame }
import org.apache.spark.mllib.linalg.{ DenseMatrix => DM, Matrix, Matrices }
import breeze.linalg.{ DenseMatrix => BDM, Matrix => BM }

trait MatrixPcaTransform extends BaseFrame {

  def matrixPca(matrixColumnName: String, vtMatrixColumnName: String): Unit = {
    execute(MatrixPca(matrixColumnName, vtMatrixColumnName))
  }
}

case class MatrixPca(matrixColumnName: String, vtMatrixColumnName: String) extends FrameTransform {

  require(matrixColumnName != null, "Matrix column mame cannot be null")
  require(vtMatrixColumnName != null, "VtMatrix column name cannot be null")

  override def work(state: FrameState): FrameState = {

    val frame = new Frame(state.rdd, state.schema)

    frame.schema.requireColumnIsType(matrixColumnName, DataTypes.matrix)
    frame.schema.requireColumnIsType(vtMatrixColumnName, DataTypes.matrix)

    frame.addColumns(MatrixPca.matrixPca(matrixColumnName, vtMatrixColumnName), Seq(Column("PrincipalComponents_" + matrixColumnName, DataTypes.matrix)))
    FrameState(frame.rdd, frame.schema)
  }

}

object MatrixPca extends Serializable {
  /**
   * Computes the principal components for each row of the frame
   *
   */

  def matrixPca(matrixColumn: String, vMatrixColumnName: String)(rowWrapper: RowWrapper): Row = {

    val matrix = rowWrapper.value(matrixColumn).asInstanceOf[DM]
    val breezeMatrix = MatrixFunctions.asBreeze(matrix)

    val vtMatrix = rowWrapper.value(vMatrixColumnName).asInstanceOf[DM]
    val breezeVtMatrix = MatrixFunctions.asBreeze(vtMatrix)

    val pca = breezeMatrix * breezeVtMatrix.t
    val pcaMatrix = MatrixFunctions.fromBreeze(pca).asInstanceOf[DM]

    Row.apply(pcaMatrix)
  }
}