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

import breeze.linalg.DenseMatrix
import org.apache.spark.mllib.linalg.{DenseMatrix, Matrix}
import org.apache.spark.mllib.linalg.{ DenseMatrix => DM, Matrix, Matrices }
import breeze.linalg.{ DenseMatrix => BDM, Matrix => BM }

object MatrixFunctions {

  /*
Creates Mllib DenseMatrix from a breeze DenseMatrix
 */
  def fromBreeze(breezeDM: BDM[Double]): Matrix = {
    new DM(breezeDM.rows, breezeDM.cols, breezeDM.data)
  }

  /*
  Creates Breeze DenseMatrix from an Mllib DenseMatrix
   */
  def asBreeze(mllibDM: DM): BDM[Double] = {
    new BDM[Double](mllibDM.numRows, mllibDM.numCols, mllibDM.values, 0, mllibDM.numCols, true)
  }
}
