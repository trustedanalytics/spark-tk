package org.trustedanalytics.sparktk.graph.internal.ops

import org.trustedanalytics.sparktk.frame.Frame
import org.apache.spark.sql.functions.{ sum, array, col, count, explode, struct }
import org.graphframes.GraphFrame
import org.apache.spark.sql.DataFrame
import org.graphframes.lib.AggregateMessages

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

import org.trustedanalytics.sparktk.graph.internal.{ GraphState, GraphSummarization, BaseGraph }

trait LabelPropagationSummarization extends BaseGraph {
  /**
   *
   * Label propagation attempts to determine communities based off of neighbor associations. A community
   * is a label that any vertex can have assigned to it, vertices are assumed to be more likely members
   * of communities they are near.
   *
   * This algorithm can fail to converge, oscillate or return the trivial solution (all members of one
   * community).
   *
   * @param maxIterations the number of iterations to run label propagation for
   * @returns dataFrame with the vertices associated with their respective communities
   */
  def labelPropagation(maxIterations: Int): Frame = {
    execute[Frame](LabelPropagation(maxIterations))
  }
}

case class LabelPropagation(maxIterations: Int) extends GraphSummarization[Frame] {

  override def work(state: GraphState): Frame = {
    new Frame(state.graphFrame.labelPropagation.maxIter(maxIterations).run())
  }
}
