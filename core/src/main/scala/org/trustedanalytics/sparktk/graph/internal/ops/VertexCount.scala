package org.trustedanalytics.sparktk.graph.internal.ops

import org.trustedanalytics.sparktk.graph.internal.{ GraphState, GraphSummarization, BaseGraph }

trait VertexCountSummarization extends BaseGraph {
  /**
   * Counts all of the rows in the frame.
   *
   * @return The number of rows in the frame.
   */
  def vertexCount(): Long = execute[Long](VertexCount)
}

/**
 * Number of rows in the current frame
 */
case object VertexCount extends GraphSummarization[Long] {
  def work(state: GraphState): Long = state.graphFrame.vertices.count()
}

