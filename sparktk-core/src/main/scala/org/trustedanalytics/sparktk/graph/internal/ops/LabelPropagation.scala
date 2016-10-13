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

trait LabelPropagationSummarization extends BaseGraph {
  /**
   *
   * Label propagation attempts to determine communities based off of neighbor associations. A community
   * is a label that any vertex can have assigned to it, vertices are assumed to be more likely members
   * of communities they are near.
   *
   * This algorithm can fail to converge, oscillate or return the trivial solution (all members of one
   * community).
   *
   * @param maxIterations the number of iterations to run label propagation for
   * @return dataFrame with the vertices associated with their respective communities
   */
  def labelPropagation(maxIterations: Int): Frame = {
    execute[Frame](LabelPropagation(maxIterations))
  }
}

case class LabelPropagation(maxIterations: Int) extends GraphSummarization[Frame] {
  require(maxIterations > 0, "maxIterations must be a positive value")

  override def work(state: GraphState): Frame = {
    new Frame(state.graphFrame.labelPropagation.maxIter(maxIterations).run())
  }
}
