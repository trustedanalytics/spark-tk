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
package org.trustedanalytics.sparktk.graph.internal

import org.apache.spark.sql.DataFrame
import org.graphframes
import org.slf4j.LoggerFactory
import org.graphframes.GraphFrame
import org.trustedanalytics.sparktk.frame.{ Frame, Schema }

/**
 * Graph schema
 */
object GraphSchema {

  // These column names must match GraphFrame exactly
  val vertexIdColumnName = GraphFrame.ID
  val edgeSourceColumnName = GraphFrame.SRC
  val edgeDestinationColumnName = GraphFrame.DST

  // These column names are used by sparktk's convention
  val vertexTypeColumnName = "vertex_"
  val edgeTypeColumnName = GraphFrame.EDGE + "_"

  implicit def frameToSchema(frame: Frame): Schema = frame.schema

  /**
   * s
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

