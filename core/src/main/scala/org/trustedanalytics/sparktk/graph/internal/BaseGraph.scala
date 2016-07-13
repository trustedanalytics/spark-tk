package org.trustedanalytics.sparktk.graph.internal

import org.slf4j.LoggerFactory
import org.graphframes.GraphFrame
import org.trustedanalytics.sparktk.frame.Frame

object GraphSchema {

  // column name that must be used if designating a vertex type
  val vertexTypeColumnName = "vertex_type"

  def validateSchemaForVertexFrame(frame: Frame) = ???
  def validateSchemaForEdgeFrame(frame: Frame) = ???
}

/**
 * State-backend for Graph
 * @param graphFrame spark graphFrame
 */
case class GraphState(graphFrame: GraphFrame)

trait BaseGraph {

  var graphState: GraphState = null

  def graphFrame: GraphFrame = if (graphState != null) graphState.graphFrame else null

  lazy val logger = LoggerFactory.getLogger("sparktk")

  private[sparktk] def init(graphFrame: GraphFrame): Unit = {
    graphState = GraphState(graphFrame)
  }

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

