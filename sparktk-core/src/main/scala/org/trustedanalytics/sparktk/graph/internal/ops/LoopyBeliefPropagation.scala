package org.trustedanalytics.sparktk.graph.internal.ops

import org.apache.spark.sql.Row
import org.trustedanalytics.sparktk.frame.Frame
import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame
import org.graphframes.GraphFrame.ID
import org.apache.spark.sql.DataFrame
import org.graphframes.lib.AggregateMessages
import org.apache.spark.sql.expressions.{ MutableAggregationBuffer, UserDefinedAggregateFunction }
import org.apache.spark.mllib.linalg.{ Vector => MLLibVector, Vectors, VectorUDT, DenseVector }
import org.apache.spark.sql.types.{ StructType, ArrayType, DoubleType }
import scala.collection.mutable.WrappedArray

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

import org.trustedanalytics.sparktk.graph.internal.{ GraphState, GraphSummarization, BaseGraph }

trait LoopyBeliefPropagationSummarization extends BaseGraph {
  /**
   * Returns a frame with annotations concerning the posterior distribution given a prior distribution.
   * This Assumes the graph is a Potts model, that is a triangle free graph with weights on each edge
   * and each vertex represents a discrete distribution, which is annotated in a property on the vertex.
   *
   * This method returns the result of the loopy belief optimizer on that Potts model given the priors.
   *
   * @param maxIterations the maximum number of iterations to run for
   * @param weight the label weight
   * @param prior The prior distribution. This is a space seperated string
   * @return The dataframe containing the vertices and their corresponding label weights
   */
  def loopyBeliefPropagation(maxIterations: Int = 10, weight: String, prior: String): Frame = {
    execute[Frame](LoopyBeliefPropagation(maxIterations, weight, prior))
  }
}

case class LoopyBeliefPropagation(maxIterations: Int, weight: String, prior: String) extends GraphSummarization[Frame] {

  val post = "_posterior"
  val margin = "_margins"
  val newMargin = "_unnormed_margins"
  val weightedMargin = "_weighted_margins"

  override def work(state: GraphState): Frame = {
    // alias to shorten code
    val AM = AggregateMessages

    val graphVertices = state.graphFrame.vertices
    state.graphFrame.edges.cache()

    // Get the number of elements in the distribution
    val firstRow = graphVertices.first
    val stateSpaceSize = splitDistribution(firstRow(firstRow.fieldIndex(prior)).toString).toArray.length

    val stateRange = (0 to stateSpaceSize - 1).toVector

    // Set up the priors, parse from string or set to uniform
    val distributionAsFloats = udf {
      (value: String) =>
        l1Normalize(splitDistribution(value).toArray)
    }

    // helper methods that do minor transformations
    val mapPriors = udf { (v: String, p: MLLibVector) => Map(v -> p) }
    val normalize = udf { (prior: MLLibVector, posterior: MLLibVector) =>
      l1Normalize(overflowProtectedProduct(List(prior, posterior)).toArray)
    }

    // parse the distribution into a vector of floats, set it up as the initial posterior
    val vertexDoubles = graphVertices
      .withColumn(prior + post, distributionAsFloats(graphVertices(prior)))
      .withColumn(prior + margin, mapPriors(col(ID), col(prior + post)))

    // Build a new graph to work on, with a proper vector of floats for the prior and posteriror
    var graphDoubles = GraphFrame(vertexDoubles, state.graphFrame.edges)

    // The main message that get's passed to each vertex
    // This is the map of each other vertex multiplied by the senders priors
    val msg = udf { (src: String, dst: String, prior: MLLibVector, marginals: Map[String, MLLibVector], edgeWeight: Double) =>

      val values = (marginals - dst).map({ case (id, values) => values }).toList
      val reducedMessages = overflowProtectedProduct(prior :: values)
      val statesUnPosteriors = stateRange.zip(reducedMessages.toArray)
      val unnormalizedMessage = stateRange.map(i => statesUnPosteriors.map({
        case (j, x: Double) =>
          x * Math.exp(potentialFunction(i, j, edgeWeight))
      }).sum)

      val message = l1Normalize(unnormalizedMessage.toArray)

      message
    }

    // Iteratively send messeges to each vertex, and aggregate those messages by multiplication. Then Post process the results
    // Based off of prior, normalize the results
    for (i <- Range(0, maxIterations)) {

      // Send mesages, aggregate. This algorithm has no direction on edges, so both source and destination are sent to
      val aggregate = graphDoubles.aggregateMessages
        .sendToSrc(msg(AM.dst(ID), AM.src(ID), AM.dst(prior + post), AM.dst(prior + margin), AM.edge(weight)))
        .sendToDst(msg(AM.src(ID), AM.dst(ID), AM.src(prior + post), AM.src(prior + margin), AM.edge(weight)))
        .agg(new VectorProduct(stateSpaceSize)(AM.msg).as(prior + newMargin))

      // move the results of this iteration to the new posterior, drop the old posterior
      val newCol = graphDoubles
        .vertices
        .join(aggregate, graphDoubles.vertices(ID) === aggregate(ID))
        .drop(aggregate(ID))
        .withColumn(prior + weightedMargin, mapPriors(col(ID), normalize(col(prior + post), col(prior + newMargin))))
        .drop(prior + newMargin)
        .drop(prior + margin)
        .withColumnRenamed(prior + weightedMargin, prior + margin)

      // Workaround for bug SPARK-13346
      val unCachedVertices = AM.getCachedDataFrame(newCol)
      graphDoubles = GraphFrame(unCachedVertices, graphDoubles.edges)
    }

    // convert the result into a python readable form
    val stringifier = udf { (posterior: Map[String, MLLibVector], index: String) => posterior(index).toString }

    // clean up intermediate values
    new Frame(graphDoubles
      .vertices
      .withColumn("Posterior", stringifier(col(prior + margin), col(ID)))
      .drop(prior + post)
      .drop(prior + margin))
  }

  // Potential function (weighted Kroenecker Delta)
  private def potentialFunction(state1: Int, state2: Int, weight: Double) = {
    if (state1 == state2) 0d else -1.0d * weight
  }

  // normalization helper methods
  private def l1Normalize(v: Array[Double]): MLLibVector = {
    val norm = l1Norm(v)
    if (norm > 0d) {
      new DenseVector(v.toArray.map(x => x / norm))
    }
    else {
      new DenseVector(v) // only happens if v is the zero vector
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
    val separatorDefault: Array[Char] = Array(' ', ',', '\t')
    new DenseVector(distribution
      .split(separatorDefault)
      .filter(_.nonEmpty)
      .map(_.toDouble))
  }

  def overflowProtectedProduct(vectors: List[MLLibVector]): MLLibVector = {
    val logs = vectors.map(x => x.toArray.toVector.map(Math.log))
    val sumOfLogs = logs.reduce(sumVectors(_, _, Double.NegativeInfinity))
    val product = sumOfLogs.map({ case x: Double => Math.exp(x) })
    new DenseVector(product.toArray)
  }

  // Helper to add two vectors elementwise
  def sumVectors(v1: Vector[Double], v2: Vector[Double], padValue: Double = 0d): Vector[Double] = {
    val length1 = v1.length
    val length2 = v2.length

    val liftedV1 = if (length1 < length2) {
      v1 ++ (1 to (length2 - length1)).map(x => padValue)
    }
    else {
      v1
    }

    val liftedV2 = if (length2 < length1) {
      v2 ++ (1 to (length1 - length2)).map(x => padValue)
    }
    else {
      v2
    }
    liftedV1.zip(liftedV2).map({ case (x, y) => x + y })
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

