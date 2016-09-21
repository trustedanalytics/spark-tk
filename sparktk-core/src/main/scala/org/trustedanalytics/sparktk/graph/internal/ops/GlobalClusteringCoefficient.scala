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
   * Returns a frame with the number of triangles each vertex is contained in
   *
   * @return The dataframe containing the vertices and their corresponding triangle counts
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
