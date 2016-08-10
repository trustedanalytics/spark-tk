package org.trustedanalytics.sparktk.models

import org.apache.spark.mllib.linalg.Matrix

/**
 * These implicits can be imported to add Matrix-related functions to model plugins
 */
object MatrixImplicits {

  implicit def matrixFunctions(matrix: Matrix): MatrixFunctions = {
    new MatrixFunctions(matrix)
  }

}