package org.trustedanalytics.sparktk.graph.internal.ops

import org.trustedanalytics.sparktk.frame.Frame
import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame
import org.graphframes.GraphFrame.ID
import org.apache.spark.sql.DataFrame
import org.graphframes.lib.AggregateMessages

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

import org.trustedanalytics.sparktk.graph.internal.{ GraphState, GraphSummarization, BaseGraph }

trait GlobalClusteringCoefficientSummarization extends BaseGraph {
  /**
   * The clustering coefficient of a graph provides a measure of how tightly
   * clustered an undirected graph is.
   *
   * More formally:
   *
   * .. math::
   *
   *    cc(G)  = \frac{ \| \{ (u,v,w) \in V^3: \ \{u,v\}, \{u, w\}, \{v,w \} \in \
   *        E \} \| }{\| \{ (u,v,w) \in V^3: \ \{u,v\}, \{u, w\} \in E \} \|}
   *
   *
   * @return The global clustering coefficient for the graph
   */
  def globalClusteringCoefficient(): Double = {
    execute[Double](GlobalClusteringCoefficient())
  }
}

case class GlobalClusteringCoefficient() extends GraphSummarization[Double] {

  val triangles = "count"
  val degree = "Degree"
  val degreeChoose2 = "DegreeChoose2"
  val vertex = "Vertex"

  override def work(state: GraphState): Double = {
    val triangleCount = state.graphFrame.triangleCount.run().agg(sum(col(triangles))).first.getLong(0)

    val chooseTwo = udf { degree: Double => if (degree <= 1) 0 else (degree * (degree - 1)) }

    val weightedDegrees: Double = state
      .graphFrame
      .aggregateMessages
      .sendToDst(lit(1))
      .sendToSrc(lit(1))
      .agg(sum(AggregateMessages.msg).as(degree)).withColumn(degreeChoose2, chooseTwo(col(degree))).agg(sum(col(degreeChoose2))).first.getDouble(0)

    (triangleCount).toDouble / (weightedDegrees.toDouble / 2)

  }

}
