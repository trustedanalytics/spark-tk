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
import org.graphframes.lib.org.trustedanalytics.{ sparktk => graphframeslib }

trait BetweennessCentralitySummarization extends BaseGraph {
  /**
   * Returns a dataframe containing a tuple of the vertex ID to the betweenness centrality of that
   * vertex
   *
   * @return The dataframe containing the vertices and their corresponding betweenness values
   */
  def betweennessCentrality(edgeWeight: Option[String] = None, normalize: Boolean = true): Frame = {
    execute[Frame](BetweennessCentrality(edgeWeight, normalize))
  }
}

case class BetweennessCentrality(edgeWeight: Option[String], normalize: Boolean) extends GraphSummarization[Frame] {

  override def work(state: GraphState): Frame = {
    new Frame(graphframeslib.BetweennessCentrality.run(state.graphFrame, edgeWeight, normalize))
  }
}
