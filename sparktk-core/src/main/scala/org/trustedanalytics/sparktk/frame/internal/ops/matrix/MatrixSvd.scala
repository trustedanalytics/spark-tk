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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.Row
import org.apache.spark.mllib.linalg.{ DenseMatrix => DM, Matrix, Matrices }
import breeze.linalg.{ DenseMatrix => BDM, Matrix => BM }
import org.trustedanalytics.sparktk.frame.{ Frame, Column, DataTypes }
import org.trustedanalytics.sparktk.frame.internal._
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd

trait MatrixSvdTransform extends BaseFrame {

  def matrixSvd(matrixColumnName: String): Unit = {
    execute(MatrixSvd(matrixColumnName))
  }
}

case class MatrixSvd(matrixColumnName: String) extends FrameTransform {

  require(matrixColumnName != null, "Matrix column name cannot be null")

  override def work(state: FrameState): FrameState = {

    val frame = new Frame(state.rdd, state.schema)

    frame.schema.requireColumnIsType(matrixColumnName, DataTypes.matrix)
    //run the operation
    frame.addColumns(MatrixSvd.matrixSvd(matrixColumnName), Seq(Column("U_" + matrixColumnName, DataTypes.matrix),
      Column("V_" + matrixColumnName, DataTypes.matrix),
      Column("SingularVectors_" + matrixColumnName, DataTypes.matrix)))
    FrameState(frame.rdd, frame.schema)

  }
}

object MatrixSvd extends Serializable {
  /**
   * Computes the singular value decomposition for each matrix of the frame
   */
  def matrixSvd(matrixColumnName: String)(rowWrapper: RowWrapper): Row = {

    val matrix = rowWrapper.value(matrixColumnName).asInstanceOf[DM]
    val breezeMatrix = MatrixFunctions.asBreeze(matrix)

    val matrixSvdResult = breeze.linalg.svd(breezeMatrix)

    val uMatrix = MatrixFunctions.fromBreeze(matrixSvdResult.U).asInstanceOf[DM]
    val vtMatrix = MatrixFunctions.fromBreeze(matrixSvdResult.Vt).asInstanceOf[DM]
    val singularVectors = new DM(1, matrixSvdResult.singularValues.length, matrixSvdResult.singularValues.toArray, false)

    Row.apply(uMatrix, vtMatrix, singularVectors)
  }

}