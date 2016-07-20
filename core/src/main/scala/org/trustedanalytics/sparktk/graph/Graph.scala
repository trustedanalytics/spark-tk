package org.trustedanalytics.sparktk.graph

import org.graphframes.examples

import org.graphframes.GraphFrame
import org.trustedanalytics.sparktk.frame.Frame
import org.trustedanalytics.sparktk.graph.internal.{ GraphState, BaseGraph }
import org.trustedanalytics.sparktk.graph.internal.ops.VertexCountSummarization

class Graph(graphFrame: GraphFrame) extends BaseGraph with Serializable
    with VertexCountSummarization {

  graphState = GraphState(graphFrame)

  //  def this(vertexFrame: Frame, egdeGrame: Frame) = {
  //    this(PythonJavaRdd.toRowRdd(jrdd.rdd, schema), schema)
  //  }
}

object Graph {
  val exampleGraphs = examples.Graphs // get example graph

  def getExampleGraphFrameFriends: GraphFrame = exampleGraphs.friends

  def createGraph(graphFrame: GraphFrame) = new Graph(graphFrame)
}
