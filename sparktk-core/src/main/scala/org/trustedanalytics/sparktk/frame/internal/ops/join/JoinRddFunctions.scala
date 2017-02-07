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
package org.trustedanalytics.sparktk.frame.internal.ops.join

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.trustedanalytics.sparktk.frame.{ SchemaHelper, FrameSchema, Column => SparkTkColumn }
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.trustedanalytics.sparktk.frame.Frame
import scala.language.implicitConversions

/**
 *
 * This is a wrapper to encapsulate methods that may need to be serialized to executed on Spark worker nodes.
 * If you don't know what this means please read about Closure Mishap
 * [[http://ampcamp.berkeley.edu/wp-content/uploads/2012/06/matei-zaharia-part-1-amp-camp-2012-spark-intro.pdf]]
 * and Task Serialization
 * [[http://stackoverflow.com/questions/22592811/scala-spark-task-not-serializable-java-io-notserializableexceptionon-when]]
 */
object JoinRddFunctions extends Serializable {

  implicit def joinRddToBroadcastJoinRddFunctions(joinParam: RddJoinParam): BroadcastJoinRddFunctions =
    new BroadcastJoinRddFunctions(joinParam)

  /**
   * Perform cross join and return a FrameRdd with the cartesian product of the right and left frames.
   *
   * If the right frame has a column with the same name as the left frame, the result frame will have a column with the
   * "_R" suffix added to the end of the column name with the right frame's column data.
   *
   * @param left Left frame for cross join
   * @param right Right frame for cross join
   * @return Result frame with the cartesian product of the left and right frames
   */
  def crossJoin(left: FrameRdd, right: FrameRdd): Frame = {
    val leftDataFrame = left.toDataFrame
    val rightDataFrame = right.toDataFrame

    // Perform cross join using the data frames
    val joinedFrame = leftDataFrame.join(rightDataFrame)

    // Create the new schema by starting with the left frame's schema
    var joinedSchema = left.frameSchema

    // Add columns from the right frame to the new new schema, while checking for duplicate column names
    right.frameSchema.columns.map(c => {
      if (joinedSchema.hasColumn(c.name)) {
        val rName = c.name + "_R"
        joinedSchema = joinedSchema.addColumnFixName(SparkTkColumn(rName, c.dataType))
      }
      else {
        joinedSchema = joinedSchema.addColumn(c)
      }
    })

    new Frame(joinedFrame.rdd, joinedSchema)
  }

  /**
   * Perform inner join
   *
   * Inner joins return all rows with matching keys in the first and second data frame.
   *
   * @param left         join parameter for first data frame
   * @param right        join parameter for second data frame
   * @param useBroadcast If one of your tables is small enough to fit in the memory of a single machine, you can use a broadcast join.
   *                     Specify which table to broadcast (left or right). Default is None.
   * @return Joined RDD
   */
  def innerJoin(left: RddJoinParam,
                right: RddJoinParam,
                useBroadcast: Option[String]): FrameRdd = {

    val joinedRdd = if (useBroadcast == Some("left") || useBroadcast == Some("right")) {
      left.innerBroadcastJoin(right, useBroadcast)
    }
    else {
      val leftFrame = left.frame
      val rightFrame = right.frame

      // Alias columns before using spark data frame join
      val (aliasedLeftFrame, aliasedRightFrame) = SchemaHelper.resolveColumnNamesConflictForJoin(leftFrame,
        rightFrame,
        leftFrame.schema.copySubset(left.joinColumns).columns.toList)

      val leftDf = aliasedLeftFrame.toDataFrame
      val rightDf = aliasedRightFrame.toDataFrame
      // If the join columns have same names in both the frames, specify the columns explicitly while
      // calling dataframe join to not have duplicate columns - cause once one has duplicate columns,
      // one can't select one out of those 2 columns. Else use expression maker.
      val joinedFrame = right.joinColumns.sorted.equals(left.joinColumns.sorted) match {
        case true => leftDf.join(rightDf, left.joinColumns)
        case false =>
          val expression = expressionMaker(leftDf, rightDf, left.joinColumns, right.joinColumns)
          leftDf.join(rightDf, expression)
      }

      // Get RDD out of the dataframe with frame columns in order
      val columnsInJoinedFrame = aliasedLeftFrame.schema.columnNames ++
        aliasedRightFrame.schema.columnNames.filterNot(right.joinColumns.contains(_))
      joinedFrame.selectExpr(columnsInJoinedFrame: _*).rdd
    }
    createJoinedFrame(joinedRdd, left, right, "inner")
  }

  /**
   * expression maker helps for generating conditions to check when join invoked with composite keys
   *
   * @param leftFrame     left data frame
   * @param rightFrame    rigth data frame
   * @param leftJoinCols  list of left frame column names used in join
   * @param rightJoinCols list of right frame column name used in join
   * @return
   */
  def expressionMaker(leftFrame: DataFrame, rightFrame: DataFrame, leftJoinCols: Seq[String], rightJoinCols: Seq[String]): Column = {
    val columnsTuple = leftJoinCols.zip(rightJoinCols)

    def makeExpression(leftCol: String, rightCol: String): Column = {
      leftFrame(leftCol).equalTo(rightFrame(rightCol))
    }
    val expression = columnsTuple.map { case (lc, rc) => makeExpression(lc, rc) }.reduce(_ && _)
    expression
  }

  /**
   * Perform left-outer join
   *
   * Left-outer join or Left join return all the rows in the first data-frame, and matching rows in the second data frame.
   *
   * @param left              join parameter for first data frame
   * @param right             join parameter for second data frame
   * @param useBroadcastRight If right table is small enough to fit in the memory of a single machine, you can set useBroadcastRight to True to perform broadcast join.
   * Default is False.
   * @return Joined RDD
   */
  def leftJoin(left: RddJoinParam,
               right: RddJoinParam,
               useBroadcastRight: Boolean): FrameRdd = {

    val joinedRdd = if (useBroadcastRight) {
      left.leftJoinBroadcastingRightTable(right)
    }
    else {
      val leftFrame = left.frame.toDataFrame
      val rightFrame = right.frame.toDataFrame
      val expression = expressionMaker(leftFrame, rightFrame, left.joinColumns, right.joinColumns)
      val joinedFrame = leftFrame.join(rightFrame,
        expression,
        joinType = "left"
      )
      joinedFrame.rdd
    }
    createJoinedFrame(joinedRdd, left, right, "left")
  }

  /**
   * Perform right-outer join
   *
   * Right-outer join or Right Join return all the rows in the second data-frame, and matching rows in the first data frame.
   *
   * @param left  join parameter for first data frame
   * @param right join parameter for second data frame
   * @param useBroadcastLeft If left table is small enough to fit in the memory of a single machine, you can set useBroadcastLeft to True to perform broadcast join.
   * Default is False.
   * @return Joined RDD
   */
  def rightJoin(left: RddJoinParam,
                right: RddJoinParam,
                useBroadcastLeft: Boolean): FrameRdd = {

    val joinedRdd = if (useBroadcastLeft) {
      left.rightJoinBroadcastingLeftTable(right)
    }
    else {
      val leftFrame = left.frame.toDataFrame
      val rightFrame = right.frame.toDataFrame
      val expression = expressionMaker(leftFrame, rightFrame, left.joinColumns, right.joinColumns)
      val joinedFrame = leftFrame.join(rightFrame,
        expression,
        joinType = "right"
      )
      joinedFrame.rdd
    }
    createJoinedFrame(joinedRdd, left, right, "right")
  }

  /**
   * Perform full-outer join
   *
   * Full-outer joins return both matching, and non-matching rows in the first and second data frame.
   * Broadcast join is not supported.
   *
   * @param left  join parameter for first data frame
   * @param right join parameter for second data frame
   * @return Joined RDD
   */
  def outerJoin(left: RddJoinParam, right: RddJoinParam): FrameRdd = {
    val leftFrame = left.frame.toDataFrame
    val rightFrame = right.frame.toDataFrame
    val expression = expressionMaker(leftFrame, rightFrame, left.joinColumns, right.joinColumns)
    val joinedFrame = leftFrame.join(rightFrame,
      expression,
      joinType = "fullouter"
    )
    createJoinedFrame(joinedFrame.rdd, left, right, "outer")
  }

  /**
   * Merge joined columns for full outer join
   *
   * Replaces null values in left join column with value in right join column
   *
   * @param joinedRdd Joined RDD
   * @param left      join parameter for first data frame
   * @param right     join parameter for second data frame
   * @return Merged RDD
   */
  def mergeJoinColumns(joinedRdd: RDD[Row],
                       left: RddJoinParam,
                       right: RddJoinParam): RDD[Row] = {

    val leftSchema = left.frame.frameSchema
    val rightSchema = right.frame.frameSchema
    val leftJoinIndices = leftSchema.columnIndices(left.joinColumns)
    val rightJoinIndices = rightSchema.columnIndices(right.joinColumns).map(rightindex => rightindex + leftSchema.columns.size)

    joinedRdd.map(row => {
      val rowArray = row.toSeq.toArray
      leftJoinIndices.zip(rightJoinIndices).foreach {
        case (leftIndex, rightIndex) => {
          if (row.get(leftIndex) == null) {
            rowArray(leftIndex) = row.get(rightIndex)
          }
        }
      }
      new GenericRow(rowArray)
    })
  }

  /**
   * Create joined frame
   *
   * The duplicate join column in the joined RDD is dropped in the joined frame.
   *
   * @param joinedRdd Joined RDD
   * @param left      join parameter for first data frame
   * @param right     join parameter for second data frame
   * @param how       join method
   * @return Joined frame
   */
  def createJoinedFrame(joinedRdd: RDD[Row],
                        left: RddJoinParam,
                        right: RddJoinParam,
                        how: String): FrameRdd = {
    how match {
      case "outer" => {
        val mergedRdd = mergeJoinColumns(joinedRdd, left, right)
        dropJoinColumn(mergedRdd, left, right, how)
      }
      case _ => {
        dropJoinColumn(joinedRdd, left, right, how)
      }
    }
  }

  /**
   * Drop duplicate data in right join column
   *
   * Used for inner, left-outer, and full-outer joins
   *
   * @param joinedRdd Joined RDD
   * @param left      join parameter for first data frame
   * @param right     join parameter for second data frame
   * @param how       Join method
   * @return Joined frame
   */
  def dropJoinColumn(joinedRdd: RDD[Row],
                     left: RddJoinParam,
                     right: RddJoinParam,
                     how: String): FrameRdd = {

    val leftSchema = left.frame.frameSchema
    val rightSchema = right.frame.frameSchema

    // Create new schema
    if (how == "inner") {
      val newRightSchema = rightSchema.dropColumns(right.joinColumns.toList)
      val newSchema = FrameSchema(SchemaHelper.join(leftSchema.columns, newRightSchema.columns).toVector)
      new FrameRdd(newSchema, joinedRdd)
    }
    else {
      val newSchema = FrameSchema(SchemaHelper.join(leftSchema.columns, rightSchema.columns).toVector)
      val frameRdd = new FrameRdd(newSchema, joinedRdd)
      val colIndices = if (how == "left" || how == "outer") {
        rightSchema.columnIndices(right.joinColumns).map(rightIndex => leftSchema.columns.size + rightIndex)
      }
      else {
        leftSchema.columnIndices(left.joinColumns)
      }
      val rightColNames = colIndices.map(colIndex => newSchema.column(colIndex).name)
      frameRdd.dropColumns(rightColNames.toList)
    }
  }
}
