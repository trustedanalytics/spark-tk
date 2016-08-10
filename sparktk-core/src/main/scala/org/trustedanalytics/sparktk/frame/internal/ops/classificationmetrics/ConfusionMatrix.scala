package org.trustedanalytics.sparktk.frame.internal.ops.classificationmetrics

case class ConfusionMatrix(rowLabels: Seq[String], columnLabels: Seq[String]) {
  val numRows = rowLabels.size
  val numColumns = columnLabels.size
  private var matrix: Array[Array[Long]] = Array.fill(numRows) { Array.fill(numColumns) { 0L } }

  def set(predictedClass: String, actualClass: String, count: Long): Unit = {
    matrix(rowIndex(actualClass))(columnIndex(predictedClass)) = count
  }

  def get(predictedClass: String, actualClass: String): Long = {
    matrix(rowIndex(actualClass))(columnIndex(predictedClass))
  }

  def getMatrix: Array[Array[Long]] = matrix

  def setMatrix(matrix: Array[Array[Long]]): Unit = {
    this.matrix = matrix
  }

  /**
   * get row index by row name
   *
   * Throws exception if not found, check first with hasColumn()
   *
   * @param rowName name of the column to find index
   */
  def rowIndex(rowName: String): Int = {
    val index = rowLabels.indexWhere(row => row == rowName, 0)
    if (index == -1)
      throw new IllegalArgumentException(s"Invalid row name $rowName provided, please choose from: " + rowLabels)
    else
      index
  }

  /**
   * get column index by column name
   *
   * Throws exception if not found, check first with hasColumn()
   *
   * @param columnName name of the column to find index
   */
  def columnIndex(columnName: String): Int = {
    val index = columnLabels.indexWhere(column => column == columnName, 0)
    if (index == -1)
      throw new IllegalArgumentException(s"Invalid column name $columnName provided, please choose from: " + columnLabels)
    else
      index
  }
}