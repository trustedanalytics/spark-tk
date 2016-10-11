/**
 *  Copyright (c) 2016 Intel Corporation 
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.trustedanalytics.sparktk.graph.internal.ops

import org.apache.spark.sql.Row
import org.trustedanalytics.sparktk.frame.Frame
import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame
import org.graphframes.GraphFrame.ID
import org.apache.spark.sql.DataFrame
import org.graphframes.lib.AggregateMessages
import org.apache.spark.sql.expressions.{ MutableAggregationBuffer, UserDefinedAggregateFunction }
import org.apache.spark.mllib.linalg.{ Vector => MLLibVector, Vectors, VectorUDT, DenseVector => MLLibDenseVector }
import org.apache.spark.sql.types.{ StructType, ArrayType, DoubleType }
import scala.collection.mutable.WrappedArray
import breeze.linalg._

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

import org.trustedanalytics.sparktk.graph.internal.{ GraphState, GraphSummarization, BaseGraph }

trait LoopyBeliefPropagationSummarization extends BaseGraph {
  /**
   * Returns a frame with annotations concerning the posterior distribution given a prior distribution.
   * This assumes the graph is a Potts model, that is a triangle free graph with weights on each edge
   * and each vertex represents a discrete distribution, which is annotated in a property on the vertex.
   *
   * This method returns the result of the loopy belief optimizer on that Potts model given the priors.
   *
   * @param maxIterations the maximum number of iterations to run for
   * @param edgeWeight the column name for the edge weight
   * @param prior The column name for the prior distribution. This is a space separated string
   * @return The dataframe containing the vertices and their corresponding label weights
   */
  def loopyBeliefPropagation(prior: String, edgeWeight: String, maxIterations: Int = 10): Frame = {
    execute[Frame](LoopyBeliefPropagation(prior, edgeWeight, maxIterations))
  }
}

case class LoopyBeliefPropagation(prior: String, edgeWeight: String, maxIterations: Int) extends GraphSummarization[Frame] {
  require(maxIterations > 0, "maxIterations must be greater than 0")

  def post(name: String) = name + "_posterior"
  def margin(name: String) = name + "_margins"
  def newMargin(name: String) = name + "_unnormed_margins"
  def weightedMargin(name: String) = name + "_weighted_margins"

  val outputName = "posterior"

  override def work(state: GraphState): Frame = {
    require(state.graphFrame.edges.columns.contains(edgeWeight), s"Property $edgeWeight not found for edge weight")
    require(state.graphFrame.vertices.columns.contains(prior), s"Property $prior not found for prior")

    val graphVertices = state.graphFrame.vertices
    state.graphFrame.edges.cache()

    // Get the number of elements in the distribution
    val firstRow = graphVertices.first
    val stateSpaceSize = splitDistribution(firstRow(firstRow.fieldIndex(prior)).toString).toArray.length

    // parse the distribution into a vector of floats, set it up as the initial posterior
    val vertexDoubles = graphVertices
      .withColumn(post(prior), distributionAsFloats(graphVertices(prior)))
      .withColumn(margin(prior), mapPriors(col(ID), col(post(prior))))

    // Build a new graph to work on, with a proper vector of floats for the prior and posteriror
    var graphDoubles = GraphFrame(vertexDoubles, state.graphFrame.edges)

    // alias to shorten code
    val AM = AggregateMessages
    // Iteratively send messeges to each vertex, and aggregate those messages by multiplication. Then Post process the results
    // Based off of prior, normalize the results
    for (i <- Range(0, maxIterations)) {

      // Send mesages, aggregate. This algorithm has no direction on edges, so both source and destination are sent to
      val aggregate = graphDoubles.aggregateMessages
        .sendToSrc(beliefPropagate(AM.dst(ID), AM.src(ID), AM.dst(post(prior)), AM.dst(margin(prior)), AM.edge(edgeWeight), lit(stateSpaceSize)))
        .sendToDst(beliefPropagate(AM.src(ID), AM.dst(ID), AM.src(post(prior)), AM.src(margin(prior)), AM.edge(edgeWeight), lit(stateSpaceSize)))
        .agg(new VectorProduct(stateSpaceSize)(AM.msg).as(newMargin(prior)))

      // move the results of this iteration to the new posterior, drop the old posterior
      val newCol = graphDoubles
        .vertices
        .join(aggregate, graphDoubles.vertices(ID) === aggregate(ID))
        .drop(aggregate(ID))
        .withColumn(weightedMargin(prior), mapPriors(col(ID), normalize(col(post(prior)), col(newMargin(prior)))))
        .drop(newMargin(prior))
        .drop(margin(prior))
        .withColumnRenamed(weightedMargin(prior), margin(prior))

      // Workaround for bug SPARK-13346
      val unCachedVertices = AM.getCachedDataFrame(newCol)
      graphDoubles = GraphFrame(unCachedVertices, graphDoubles.edges)
    }

    // convert the result into a python readable form
    val stringifier = udf { (posterior: Map[String, MLLibVector], index: String) => posterior(index).toString }

    // clean up intermediate values
    new Frame(graphDoubles
      .vertices
      .withColumn(outputName, stringifier(col(margin(prior)), col(ID)))
      .drop(post(prior))
      .drop(margin(prior)))
  }

  // Set up the priors, parse from string or set to uniform
  val distributionAsFloats = udf {
    (value: String) =>
      l1Normalize(splitDistribution(value).toArray)
  }

  // helper methods that do minor transformations
  val mapPriors = udf { (v: String, p: MLLibVector) => Map(v -> p) }
  val normalize = udf { (prior: MLLibVector, posterior: MLLibVector) =>
    l1Normalize((new DenseVector(prior.toArray) :* new DenseVector(posterior.toArray)).toArray)
  }

  // The main message that get's passed to each vertex
  // This is the map of each other vertex multiplied by the senders priors
  private val beliefPropagate = udf { (src: String, dst: String, prior: MLLibVector, marginals: Map[String, MLLibVector], edgeWeight: Double, stateSpaceSize: Int) =>

    val stateRange = (0 to stateSpaceSize - 1).toVector

    val value = (marginals - dst).map({ case (id, values) => new DenseVector(values.toArray) }).toList.reduce((x, y) => x :* y)
    val reducedMessages = (new DenseVector(prior.toArray) :* value)
    val statesUnPosteriors = stateRange.zip(reducedMessages.toArray)
    val unnormalizedMessage = stateRange.map(i => statesUnPosteriors.map({
      case (j, x: Double) =>
        x * Math.exp(potentialFunction(i, j, edgeWeight))
    }).sum)

    val message = l1Normalize(unnormalizedMessage.toArray)

    message
  }

  // Potential function (weighted Kroenecker Delta)
  private def potentialFunction(state1: Int, state2: Int, weight: Double) = {
    if (state1 == state2) 0d else -1.0d * weight
  }

  // normalization helper methods
  private def l1Normalize(v: Array[Double]): MLLibVector = {
    val norm = l1Norm(v)
    if (norm > 0d) {
      new MLLibDenseVector(v.toArray.map(x => x / norm))
    }
    else {
      new MLLibDenseVector(v) // only happens if v is the zero vector
    }
  }

  private def l1Norm(v: Array[Double]): Double = {
    if (v.toArray.isEmpty) {
      0d
    }
    else {
      v.map(x => Math.abs(x)).sum
    }
  }

  // This helper method splits our string representation of a distribution and converts it to a vector of doubles
  private def splitDistribution(distribution: String): MLLibVector = {
    val separatorDefault: String = "\\s+"
    new MLLibDenseVector(distribution
      .split(separatorDefault)
      .map(_.toDouble))
  }

}

// Custom aggregate for multiplying MLLib vectors
class VectorProduct(n: Int) extends UserDefinedAggregateFunction {
  def inputSchema = new StructType().add("v", new VectorUDT())
  def bufferSchema = new StructType().add("buff", ArrayType(DoubleType))
  def dataType = new VectorUDT()
  def deterministic = true

  def initialize(buffer: MutableAggregationBuffer) = {
    buffer.update(0, Array.fill(n)(1.0))
  }

  def update(buffer: MutableAggregationBuffer, input: Row) = {
    if (!input.isNullAt(0)) {
      val buff = buffer.getAs[WrappedArray[Double]](0)
      val v = input.getAs[MLLibVector](0).toSparse
      for (i <- v.indices) {
        buff(i) *= v(i)
      }
      buffer.update(0, buff)
    }
  }

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {
    val buff1 = buffer1.getAs[WrappedArray[Double]](0)
    val buff2 = buffer2.getAs[WrappedArray[Double]](0)
    for ((x, i) <- buff2.zipWithIndex) {
      buff1(i) *= x
    }
    buffer1.update(0, buff1)
  }

  def evaluate(buffer: Row) = Vectors.dense(buffer.getAs[Seq[Double]](0).toArray)
}

