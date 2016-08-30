package org.trustedanalytics.sparktk.graph.internal.ops

import org.trustedanalytics.sparktk.frame.Frame
import org.apache.spark.sql.functions.{ sum, array, col, count, explode, struct }
import org.graphframes.GraphFrame
import org.apache.spark.sql.DataFrame
import org.graphframes.lib.AggregateMessages

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

import org.trustedanalytics.sparktk.graph.internal.{ GraphState, GraphSummarization, BaseGraph }

trait ConnectedComponentsSummarization extends BaseGraph {
  /**
   *
   * Connected components are disjoint subgraphs in which all vertices are
   * connected to all other vertices in the same component via paths, but not
   * connected via paths to vertices in any other component.
   *
   * @return The dataframe containing the vertices and which component they belong to
   */
  def connectedComponents(): Frame = {
    execute[Frame](ConnectedComponents())
  }
}

case class ConnectedComponents() extends GraphSummarization[Frame] {

  override def work(state: GraphState): Frame = {
    new Frame(state.graphFrame.connectedComponents.run.toDF("Vertex", "Component"))
  }
}
