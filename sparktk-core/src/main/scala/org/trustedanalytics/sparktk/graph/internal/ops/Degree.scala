package org.trustedanalytics.sparktk.graph.internal.ops

import org.trustedanalytics.sparktk.frame.Frame
import org.apache.spark.sql.functions.{ sum, array, col, count, explode, struct }
import org.graphframes.GraphFrame
import org.apache.spark.sql.DataFrame
import org.graphframes.lib.AggregateMessages

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

import org.trustedanalytics.sparktk.graph.internal.{ GraphState, GraphSummarization, BaseGraph }

trait DegreeSummarization extends BaseGraph {
  /**
   * Returns a frame with annotations concerning the degree of each node,
   * weighted by a given edge weight
   *
   * @param degreeOption One of "in", "out" or "undirected". Determines the edge direction for the degree
   * @return The dataframe containing the vertices and their corresponding weights
   */
  def degree(degreeOption: String = "undirected"): Frame = {
    execute[Frame](Degree(degreeOption))
  }
}

case class Degree(degreeOption: String) extends GraphSummarization[Frame] {
  val grouper = "degree_groupby"
  require(degreeOption == "in" || degreeOption == "out" || degreeOption == "undirected", "Invalid degree option, please choose \"in\", \"out\", or \"undirected\"")

  override def work(state: GraphState): Frame = {
    val (dstMsg, srcMsg) = degreeOption match {
      case "in" => (lit(1), lit(0))
      case "out" => (lit(0), lit(1))
      case "undirected" => (lit(1), lit(1))
    }
    val degrees = state.graphFrame.aggregateMessages.sendToDst(dstMsg).sendToSrc(srcMsg).agg(sum(AggregateMessages.msg))
    val degreesFrame = degrees.toDF("Vertex", "Degree")
    new Frame(degreesFrame)
  }
}
