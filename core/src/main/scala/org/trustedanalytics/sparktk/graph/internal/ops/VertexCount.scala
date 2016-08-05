package org.trustedanalytics.sparktk.graph.internal.ops

import org.trustedanalytics.sparktk.graph.internal.{ GraphState, GraphSummarization, BaseGraph }

trait VertexCountSummarization extends BaseGraph {
  /**
   * Counts all of the rows in the vertices frame.
   *
   * @return The number of rows in the frame.
   */
  def vertexCount(): Long = execute[Long](VertexCount)
}

case object VertexCount extends GraphSummarization[Long] {
  def work(state: GraphState): Long = state.graphFrame.vertices.count()
}

