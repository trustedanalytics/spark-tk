package org.trustedanalytics.sparktk.models.dimreduction.pca

import org.json4s.JsonAST.JValue

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{ Vector => MllibVector, Vectors => MllibVectors, Matrix => MllibMatrix, Matrices => MllibMatrices, DenseVector }
import org.apache.spark.mllib.linalg.distributed.{ RowMatrix, IndexedRow, IndexedRowMatrix }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.trustedanalytics.sparktk.TkContext

import org.trustedanalytics.sparktk.frame._
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.trustedanalytics.sparktk.saveload.{ TkSaveLoad, SaveLoad, TkSaveableObject }

object PcaModel extends TkSaveableObject {

  /**
   * Create a new PcaModel and train it according to the given arguments
   *
   * @param frame The frame containing the data to train on
   * @param columns The columns to train on
   * @param meanCentered Option to mean center the columns
   * @param k Principal component count. Default is the number of observation columns
   */
  def train(frame: Frame,
            columns: Seq[String],
            meanCentered: Boolean = true,
            k: Option[Int] = None): PcaModel = {
    require(frame != null, "frame is required")
    require(columns != null, "data columns names cannot be null")
    frame.schema.requireColumnsAreVectorizable(columns)
    require(k != null)
    if (k.isDefined) {
      require(k.get <= columns.length, s"k=$k must be less than or equal to the number of observation columns=${columns.length}")
      require(k.get >= 1, s"number of Eigen values to use (k=$k) must be greater than equal to 1.")
    }
    val trainFrameRdd = new FrameRdd(frame.schema, frame.rdd)

    val train_k = k.getOrElse(columns.length)

    val rowMatrix = PrincipalComponentsFunctions.toRowMatrix(trainFrameRdd, columns, meanCentered)
    val svd = rowMatrix.computeSVD(train_k, computeU = false)
    val columnStatistics = trainFrameRdd.columnStatistics(columns)

    var singularValues = svd.s
    while (singularValues.size < train_k) {
      // todo: add logging for this case, where the value count is less than k (could happen for toy data --i.e. it did)
      singularValues = new DenseVector(singularValues.toArray :+ 0.0)
    }

    PcaModel(columns, meanCentered, train_k, columnStatistics.mean, singularValues, svd.V)
  }

  def loadTkSaveableObject(sc: SparkContext, path: String, formatVersion: Int, tkMetadata: JValue): Any = {
    validateFormatVersion(formatVersion, 1)
    val json: PcaModelJson = SaveLoad.extractFromJValue[PcaModelJson](tkMetadata)
    json.toPcaModel
  }

  /**
   * Load a PcaModel from the given path
   * @param tc TkContext
   * @param path location
   * @return
   */
  def load(tc: TkContext, path: String): PcaModel = {
    tc.load(path).asInstanceOf[PcaModel]
  }
}

/**
 * A PCA Model, holding all the important parameters
 *
 * @param columns sequence of observation columns of the data frame
 * @param meanCentered Indicator whether the columns were mean centered for training
 * @param k Principal component count
 * @param columnMeans Means of the columns
 * @param singularValues Singular values of the specified columns in the input frame
 * @param rightSingularVectors Right singular vectors of the specified columns in the input frame
 */
case class PcaModel private[pca] (columns: Seq[String],
                                  meanCentered: Boolean,
                                  k: Int,
                                  columnMeans: MllibVector,
                                  singularValues: MllibVector,
                                  rightSingularVectors: MllibMatrix) extends Serializable {
  /**
   * Saves this model to a file
   *
   * @param sc active SparkContext
   * @param path save to path
   */
  def save(sc: SparkContext, path: String): Unit = {
    val formatVersion: Int = 1
    val jsonable = PcaModelJson(columns,
      meanCentered,
      k,
      columnMeans.toArray,
      singularValues.toArray,
      rightSingularVectors.numRows,
      rightSingularVectors.numCols,
      rightSingularVectors.toArray)
    TkSaveLoad.saveTk(sc, path, PcaModel.formatId, formatVersion, jsonable)
  }

  def columnMeansAsArray: Array[Double] = columnMeans.toArray

  def singularValuesAsArray: Array[Double] = singularValues.toArray

  def rightSingularVectorsAsArray: Array[Double] = {
    rightSingularVectors.transpose.toArray
  }

  /**
   *
   * @param frame frame on which to base predictions and to which the predictions will be added
   * @param columns the observation columns in that frame, must be equal to the number of columns this model was trained with.  Default is the same names as the train frame.
   * @param meanCentered whether to mean center the columns. Default is true
   * @param k the number of principal components to be computed, must be <= the k used in training.  Default is the trained k
   * @param tSquaredIndex whether the t-square index is to be computed. Default is false
   */
  def predict(frame: Frame,
              columns: Option[List[String]] = None,
              meanCentered: Boolean = true,
              k: Option[Int] = None,
              tSquaredIndex: Boolean = false): Unit = {

    if (meanCentered) {
      require(this.meanCentered, "Cannot mean center the predict frame if the train frame was not mean centered.")
    }
    val predictColumns = columns.getOrElse(this.columns)
    require(predictColumns.length == this.columns.length, "Number of columns for train and predict should be same")
    val predictK = k.getOrElse(this.k)
    require(predictK <= this.k, s"Number of components ($predictK) must be at most the number of components trained on ($this.k)")

    val frameRdd = new FrameRdd(frame.schema, frame.rdd)
    val indexedRowMatrix = PrincipalComponentsFunctions.toIndexedRowMatrix(frameRdd, predictColumns, meanCentered)
    val principalComponents = PrincipalComponentsFunctions.computePrincipalComponents(rightSingularVectors, predictK, indexedRowMatrix)

    val pcaColumns = for (i <- 1 to predictK) yield Column("p_" + i.toString, DataTypes.float64)
    val (componentColumns, components) = tSquaredIndex match {
      case true =>
        val tSquareMatrix = PrincipalComponentsFunctions.computeTSquaredIndex(principalComponents, singularValues, predictK)
        val tSquareColumn = Column("t_squared_index", DataTypes.float64)
        (pcaColumns :+ tSquareColumn, tSquareMatrix)
      case false => (pcaColumns, principalComponents)
    }

    val componentRows = components.rows.map(row => Row.fromSeq(row.vector.toArray.toSeq))
    val componentFrame = new FrameRdd(FrameSchema(componentColumns), componentRows)
    val resultFrameRdd = frameRdd.zipFrameRdd(componentFrame)
    frame.init(resultFrameRdd.rdd, resultFrameRdd.schema)
  }
}

object PrincipalComponentsFunctions extends Serializable {

  /**
   * Compute principal components using trained model
   *
   * @param eigenVectors Right singular vectors of the specified columns in the input frame
   * @param c Number of principal components to compute
   * @param indexedRowMatrix  Indexed row matrix with input data
   * @return Principal components with projection of input into k dimensional space
   */
  def computePrincipalComponents(eigenVectors: MllibMatrix,
                                 c: Int,
                                 indexedRowMatrix: IndexedRowMatrix): IndexedRowMatrix = {
    val y = indexedRowMatrix.multiply(eigenVectors)
    val cComponentsOfY = new IndexedRowMatrix(y.rows.map(r => r.copy(vector = MllibVectors.dense(r.vector.toArray.take(c)))))
    cComponentsOfY
  }

  /**
   * Compute the t-squared index for an IndexedRowMatrix created from the input frame
   * @param y IndexedRowMatrix storing the projection into k dimensional space
   * @param E Singular Values
   * @param k Number of dimensions
   * @return IndexedRowMatrix with existing elements in the RDD and computed t-squared index
   */
  def computeTSquaredIndex(y: IndexedRowMatrix, E: MllibVector, k: Int): IndexedRowMatrix = {
    val matrix = y.rows.map(row => {
      val rowVectorToArray = row.vector.toArray
      var t = 0d
      for (i <- 0 until k) {
        if (E(i) > 0)
          t += ((rowVectorToArray(i) * rowVectorToArray(i)) / (E(i) * E(i)))
      }
      new IndexedRow(row.index, MllibVectors.dense(rowVectorToArray :+ t))
    })
    new IndexedRowMatrix(matrix)
  }

  /**
   * Convert frame to distributed row matrix
   *
   * @param frameRdd Input frame
   * @param columns List of columns names for creating row matrix
   * @param meanCentered If true, mean center the columns
   * @return Distributed row matrix
   */
  def toRowMatrix(frameRdd: FrameRdd, columns: Seq[String], meanCentered: Boolean): RowMatrix = {
    val vectorRdd = toVectorRdd(frameRdd, columns, meanCentered)
    new RowMatrix(vectorRdd)
  }

  /**
   * Convert frame to distributed indexed row matrix
   *
   * @param frameRdd Input frame
   * @param columns List of columns names for creating row matrix
   * @param meanCentered If true, mean center the columns
   * @return Distributed indexed row matrix
   */
  def toIndexedRowMatrix(frameRdd: FrameRdd, columns: Seq[String], meanCentered: Boolean): IndexedRowMatrix = {
    val vectorRdd = toVectorRdd(frameRdd, columns, meanCentered)
    new IndexedRowMatrix(vectorRdd.zipWithIndex().map { case (vector, index) => IndexedRow(index, vector) })
  }

  /**
   * Convert frame to vector RDD
   *
   * @param frameRdd Input frame
   * @param columns List of columns names for vector RDD
   * @param meanCentered If true, mean center the columns
   * @return Vector RDD
   */
  def toVectorRdd(frameRdd: FrameRdd, columns: Seq[String], meanCentered: Boolean): RDD[MllibVector] = {
    if (meanCentered) {
      frameRdd.toMeanCenteredDenseVectorRdd(columns)
    }
    else {
      frameRdd.toDenseVectorRdd(columns)
    }
  }
}

/**
 * PcaModel's important content represented in a case class that is easily JSON serializable
 *
 * @param columns sequence of observation columns of the data frame
 * @param meanCentered Indicator whether the columns were mean centered for training
 * @param k Principal component count
 * @param columnMeans Means of the columns
 * @param singularValues Singular values of the specified columns in the input frame
 * @param rsvNumRows number of rows in the right singular vectors matrix
 * @param rsvNumCols number of cols in the right singular vectors matrix
 * @param rsvValues Right singular vectors of the specified columns in the input frame
 */
case class PcaModelJson(columns: Seq[String],
                        meanCentered: Boolean,
                        k: Int,
                        columnMeans: Array[Double],
                        singularValues: Array[Double],
                        rsvNumRows: Int,
                        rsvNumCols: Int,
                        rsvValues: Array[Double]) extends Serializable {

  def toPcaModel: PcaModel = {
    val meansVector: MllibVector = MllibVectors.dense(columnMeans)
    val sv: MllibVector = MllibVectors.dense(singularValues)
    val rsv: MllibMatrix = MllibMatrices.dense(rsvNumRows, rsvNumCols, rsvValues)
    PcaModel(columns, meanCentered, k, meansVector, sv, rsv)
  }
}
