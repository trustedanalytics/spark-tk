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

trait ClusteringCoefficientSummarization extends BaseGraph {
  /**
   * Returns a frame with the number of triangles each vertex is contained in
   *
   * @return The dataframe containing the vertices and their corresponding triangle counts
   */
  def clusteringCoefficient(): Frame = {
    execute[Frame](ClusteringCoefficient())
  }
}

case class ClusteringCoefficient() extends GraphSummarization[Frame] {

  val triangles = "count"
  val degree = "Degree"
  val vertex = "Vertex"

  override def work(state: GraphState): Frame = {
    val triangleCount = state.graphFrame.triangleCount.run()

    val weightedDegrees = state
      .graphFrame
      .aggregateMessages
      .sendToDst(lit(1))
      .sendToSrc(lit(1))
      .agg(sum(AggregateMessages.msg).as(degree))

    val joinedFrame = weightedDegrees
      .join(triangleCount, weightedDegrees(ID) === triangleCount(ID))
      .drop(weightedDegrees(ID))

    val msg = udf { (triangles: Int, degree: Int) => if (degree <= 1) 0.0d else (triangles * 2).toDouble / (degree * (degree - 1)).toDouble }

    val clusteringVertices = joinedFrame
      .withColumn("Clustering_Coefficient", msg(col(triangles), col(degree)))
      .drop(col(triangles))
      .drop(col(degree))
      .withColumnRenamed(ID, vertex)

    new Frame(clusteringVertices)

  }

}
