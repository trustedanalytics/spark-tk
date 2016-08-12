package org.trustedanalytics.sparktk.graph.internal.ops

import org.trustedanalytics.sparktk.frame.Frame
import org.apache.spark.sql.functions.{ sum, array, col, count, explode, struct }
import org.graphframes.GraphFrame
import org.apache.spark.sql.DataFrame
import org.graphframes.lib.AggregateMessages

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

import org.trustedanalytics.sparktk.graph.internal.{ GraphState, GraphSummarization, BaseGraph }

trait WeightedDegreeSummarization extends BaseGraph {
  /**
   * Returns a graph with annotations concerning the degree of each node,
   * weighted by a given edge weight
   *
   * @param edgeWeight the name of the weight value in the edge
   * @return The dataframe containing the vertices and their corresponding weights
   */
  def weightedDegree(edgeWeight: String, degreeOption: String, defaultWeight: Float): Frame = {
    execute[Frame](WeightedDegree(edgeWeight, degreeOption, defaultWeight))
  }
}

case class WeightedDegree(edgeWeight: String, degreeOption: String, defaultWeight: Float) extends GraphSummarization[Frame] {
  val grouper = "weighted_degree_groupby"
  require(degreeOption == "in" || degreeOption == "out" || degreeOption == "undirected")

  override def work(state: GraphState): Frame = {
    require(state.graphFrame.edges.columns.contains(edgeWeight))
    val graphFrame = state.graphFrame
    // If you are counting the in Degrees you are looking at the destination of
    // the edges, if you are looking at the out degrees you are looking at the
    // source
    val (dstMsg, srcMsg) = degreeOption match {
      case "in" => (AggregateMessages.edge(edgeWeight), lit(0))
      case "out" => (lit(0), AggregateMessages.edge(edgeWeight))
      case "undirected" => (AggregateMessages.edge(edgeWeight), AggregateMessages.edge(edgeWeight))
    }
    val weightedDegrees = graphFrame.aggregateMessages.sendToDst(dstMsg).sendToSrc(srcMsg).agg(sum(AggregateMessages.msg))
    val degreesFrame = weightedDegrees.toDF("Node", "Degree")
    new Frame(degreesFrame)
  }
}
