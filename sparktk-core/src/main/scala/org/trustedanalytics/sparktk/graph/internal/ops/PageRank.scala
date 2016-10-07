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

import org.trustedanalytics.sparktk.frame.Frame
import org.apache.spark.sql.functions.{ sum, array, col, count, explode, struct }
import org.graphframes.GraphFrame
import org.apache.spark.sql.DataFrame
import org.graphframes.lib.AggregateMessages

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

import org.trustedanalytics.sparktk.graph.internal.{ GraphState, GraphSummarization, BaseGraph }

trait PageRankSummarization extends BaseGraph {
  /**
   * Calculates the page rank for each vertex
   *
   *    **Basics and Background**
   *
   *    *PageRank* is a method for determining which vertices in a directed graph are
   *    the most central or important.
   *    *PageRank* gives each vertex a score which can be interpreted as the
   *    probability that a person randomly walking along the edges of the graph will
   *    visit that vertex.
   *
   *    The calculation of *PageRank* is based on the supposition that if a vertex has
   *    many vertices pointing to it, then it is "important",
   *    and that a vertex grows in importance as more important vertices point to it.
   *    The calculation is based only on the network structure of the graph and makes
   *    no use of any side data, properties, user-provided scores or similar
   *    non-topological information.
   *
   *    *PageRank* was most famously used as the core of the Google search engine for
   *    many years, but as a general measure of :term:`centrality` in a graph, it has
   *    other uses to other problems, such as :term:`recommendation systems` and
   *    analyzing predator-prey food webs to predict extinctions.
   *
   *    **Background references**
   *
   *   Basic description and principles: `Wikipedia\: PageRank`_
   *   Applications to food web analysis: `Stanford\: Applications of PageRank`_
   *   Applications to recommendation systems: `PLoS\: Computational Biology`_
   *
   * NOTE:: exactly one of maxIterations and convergenceTolerance must be set
   *
   * @param maxIterations The maximum number of iterations to run for. Mutually exclusive with convergenceTolerance
   * @param resetProbability The reset probability for the page rank equation
   * @param convergenceTolerance If the difference between two iterations of page rank is less than this, page rank will terminate. Mutually exclusive with maxIterations
   * @return The dataframe containing the vertices and their corresponding page rank
   */
  def pageRank(maxIterations: Option[Int], resetProbability: Option[Double], convergenceTolerance: Option[Double]): Frame = {
    execute[Frame](PageRank(maxIterations, resetProbability, convergenceTolerance))
  }
}

case class PageRank(maxIterations: Option[Int], resetProbability: Option[Double], convergenceTolerance: Option[Double]) extends GraphSummarization[Frame] {

  override def work(state: GraphState): Frame = {
    require(maxIterations.isEmpty || convergenceTolerance.isEmpty, "Only set one of max iterations or convergence tolerance")
    require(!maxIterations.isEmpty || !convergenceTolerance.isEmpty, "Must set one of max iterations or convergence tolerance")

    val pageRank = state.graphFrame.pageRank

    // Set the relevant values

    val setTol = convergenceTolerance.fold(pageRank)(pageRank.tol)

    val setMaxIterations = maxIterations.fold(pageRank)(pageRank.maxIter)

    val setReset = resetProbability.fold(pageRank)(pageRank.resetProbability)

    new Frame(pageRank.run().vertices)
  }
}
