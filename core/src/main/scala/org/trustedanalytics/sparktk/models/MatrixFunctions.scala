package org.trustedanalytics.sparktk.models

import org.apache.spark.mllib.linalg.Matrix

import scala.collection.mutable.ListBuffer

class MatrixFunctions(self: Matrix) {
  /**
   * Convert Matrix to List[List[Double]  ]
   */
  def toListOfList(): List[List[Double]] = {
    var matrixList = new ListBuffer[List[Double]]()
    for (i <- 0 to self.numRows - 1) {
      var rowList = new ListBuffer[Double]()
      for (j <- 0 to self.numCols - 1) {
        rowList += self(i, j)
      }
      matrixList += rowList.toList
    }
    matrixList.toList
  }

}

