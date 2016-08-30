package org.trustedanalytics.sparktk.graph.internal.ops

import org.trustedanalytics.sparktk.frame.Frame
import org.apache.spark.sql.functions.{ sum, array, col, count, explode, struct }
import org.graphframes.GraphFrame
import org.apache.spark.sql.DataFrame
import org.graphframes.lib.AggregateMessages

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

import org.trustedanalytics.sparktk.graph.internal.{ GraphState, GraphSummarization, BaseGraph }

trait TriangleCountSummarization extends BaseGraph {
  /**
   * Returns a frame with the number of triangles each vertex is contained in
   *
   * @return The dataframe containing the vertices and their corresponding triangle counts
   */
  def triangleCount(): Frame = {
    execute[Frame](TriangleCount())
  }
}

case class TriangleCount() extends GraphSummarization[Frame] {

  override def work(state: GraphState): Frame = {
    new Frame(state.graphFrame.triangleCount.run().toDF("Triangles", "Vertex"))
  }
}
