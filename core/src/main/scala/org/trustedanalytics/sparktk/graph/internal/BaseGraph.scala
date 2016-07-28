package org.trustedanalytics.sparktk.graph.internal

import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory
import org.graphframes.GraphFrame
import org.trustedanalytics.sparktk.frame.{ Frame, Schema }

/**
 * Graph schema
 */
object GraphSchema {

  // These column names must match GraphFrame exactly
  val vertexIdColumnName = "id"
  val edgeSourceColumnName = "src"
  val edgeDestinationColumnName = "dst"

  // These column names are used by sparktk's convention
  val vertexTypeColumnName = "vertex_type"
  val edgeTypeColumnName = "edge_type"

  implicit def frameToSchema(frame: Frame): Schema = frame.schema

  /**
   * Validates the frame has the proper schema to represent a Vertices Frame
   */
  def validateSchemaForVerticesFrame(frameSchema: Schema) = {
    frameSchema.validateColumnsExist(List(vertexIdColumnName))
  }

  /**
   * Validates the frame has the proper schema to represent an Edges Frame
   */
  def validateSchemaForEdgesFrame(frameSchema: Schema) = {
    frameSchema.validateColumnsExist(List(edgeSourceColumnName, edgeDestinationColumnName))
  }

  /**
   * Validates the dataframe has the proper schema to represent a Vertices Frame
   */
  def validateSchemaForVerticesFrame(df: DataFrame) = {
    require(df.columns.contains(vertexIdColumnName))
  }

  /**
   * Validates the dataframe has the proper schema to represent an Edges Frame
   */
  def validateSchemaForEdgesFrame(df: DataFrame) = {
    require(df.columns.contains(edgeSourceColumnName), s"Schema must contain column named $edgeSourceColumnName")
    require(df.columns.contains(edgeDestinationColumnName), s"Schema must contain column named $edgeDestinationColumnName")
  }
}

/**
 * State-backend for Graph
 *
 * @param graphFrame spark graphFrame
 */
case class GraphState(graphFrame: GraphFrame)

/**
 * Base Trait
 */
trait BaseGraph {

  var graphState: GraphState = null

  def graphFrame: GraphFrame = if (graphState != null) graphState.graphFrame else null

  lazy val logger = LoggerFactory.getLogger("sparktk")

  protected def execute[T](summarization: GraphSummarization[T]): T = {
    logger.info("Graph summarization {}", summarization.getClass.getName)
    summarization.work(graphState)
  }
}

trait GraphOperation extends Product {
  //def name: String
}

trait GraphSummarization[T] extends GraphOperation {
  def work(state: GraphState): T
}

