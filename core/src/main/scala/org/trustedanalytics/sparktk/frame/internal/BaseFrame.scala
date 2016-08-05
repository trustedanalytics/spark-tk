package org.trustedanalytics.sparktk.frame.internal

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ SQLContext, DataFrame, Row }

import org.slf4j.LoggerFactory
import org.trustedanalytics.sparktk.frame.Schema
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd

import scala.util.{ Failure, Success }

trait BaseFrame {

  private var frameState: FrameState = null

  lazy val logger = LoggerFactory.getLogger("sparktk")

  /**
   * The content of the frame as an RDD of Rows.
   */
  def rdd: RDD[Row] = if (frameState != null) frameState.rdd else null

  /**
   * Current frame column names and types.
   */
  def schema: Schema = if (frameState != null) frameState.schema else null

  /**
   * The content of the frame as a Spark DataFrame
   */
  def dataframe: DataFrame = if (frameState != null) {
    val frameRdd = new FrameRdd(schema, rdd)
    frameRdd.toDataFrame
  }
  else null

  /**
   * Validates the data against the specified schema. Attempts to parse the data to the column's data type.  If
   * it's unable to parse the data to the specified data type, it's replaced with null.
   *
   * @param rddToValidate RDD of data to validate against the specified schema
   * @param schemaToValidate Schema to use to validate the data
   * @return RDD that has data parsed to the schema's data types
   */
  protected def validateSchema(rddToValidate: RDD[Row], schemaToValidate: Schema): SchemaValidationReturn = {
    val columnCount = schemaToValidate.columns.length
    val schemaWithIndex = schemaToValidate.columns.zipWithIndex

    val badValueCount = rddToValidate.sparkContext.accumulator(0, "Frame bad values")

    val validatedRdd = rddToValidate.map(row => {
      if (row.length != columnCount)
        throw new RuntimeException(s"Row length of ${row.length} does not match the number of columns in the schema (${columnCount}).")

      val parsedValues = schemaWithIndex.map {
        case (column, index) =>
          column.dataType.parse(row.get(index)) match {
            case Success(value) => value
            case Failure(e) =>
              badValueCount += 1
              null
          }
      }

      Row.fromSeq(parsedValues)
    })

    // Call count() to force rdd map to execute so that we can get the badValueCount from the accumulator.
    validatedRdd.count()

    SchemaValidationReturn(validatedRdd, ValidationReport(badValueCount.value))
  }

  private[sparktk] def init(rdd: RDD[Row], schema: Schema): Unit = {
    frameState = FrameState(rdd, schema)
  }

  protected def execute(transform: FrameTransform): Unit = {
    logger.info("Frame transform {}", transform.getClass.getName)
    frameState = transform.work(frameState)
  }

  protected def execute[T](summarization: FrameSummarization[T]): T = {
    logger.info("Frame summarization {}", summarization.getClass.getName)
    summarization.work(frameState)
  }

  protected def execute[T](transform: FrameTransformWithResult[T]): T = {
    logger.info("Frame transform (with result) {}", transform.getClass.getName)
    val r = transform.work(frameState)
    frameState = r.state
    r.result
  }
}

/**
 * Validation report for schema and rdd validation.
 *
 * @param numBadValues The number of values that were unable to be parsed to the column's data type.
 */
case class ValidationReport(numBadValues: Int)

/**
 * Value to return from the function that validates the data against schema.
 *
 * @param validatedRdd RDD of data has been casted to the data types specified by the schema.
 * @param validationReport Validation report specifying how many values were unable to be parsed to the column's
 *                         data type.
 */
case class SchemaValidationReturn(validatedRdd: RDD[Row], validationReport: ValidationReport)

trait FrameOperation extends Product {
  //def name: String
}

trait FrameTransform extends FrameOperation {
  def work(state: FrameState): FrameState
}

case class FrameTransformReturn[T](state: FrameState, result: T)

trait FrameTransformWithResult[T] extends FrameOperation {
  def work(state: FrameState): FrameTransformReturn[T]
}

trait FrameSummarization[T] extends FrameOperation {
  def work(state: FrameState): T
}

trait FrameCreation extends FrameOperation {
  def work(): FrameState
}
