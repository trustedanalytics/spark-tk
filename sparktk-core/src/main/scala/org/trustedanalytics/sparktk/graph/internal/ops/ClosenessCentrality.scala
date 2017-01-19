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
import org.trustedanalytics.sparktk.graph.internal.{ BaseGraph, GraphSummarization, GraphState }
import org.graphframes.lib.org.trustedanalytics.{ sparktk => graphframeslib }

trait ClosenessCentralitySummarization extends BaseGraph {

  /**
   * Compute closeness centrality for nodes.
   *
   * Closeness centrality of a node is the reciprocal of the sum of the shortest path distances from this node to all
   * other nodes in the graph. Since the sum of distances depends on the number of nodes in the
   * graph, closeness is normalized by the sum of minimum possible distances.
   *
   * In the case of a disconnected graph, the algorithm computes the closeness centrality for each connected part.
   *
   * In the case of a weighted graph, the algorithm handles only positive edge weights and uses Dijkstra's algorithm for
   * the shortest-path calculations
   *
   * Reference: Linton C. Freeman: Centrality in networks: I.Conceptual clarification. Social Networks 1:215-239, 1979.
   * http://leonidzhukov.ru/hse/2013/socialnetworks/papers/freeman79-centrality.pdf
   *
   * @param edgePropName optional edge column name to be used as edge weight
   * @param normalize if true, normalizes the closeness centrality value to the number of nodes connected to it
   *                   divided by the total number of nodes in the graph, this is effective in the case of
   *                   disconnected graph
   * @return frame with an additional column for the closeness centrality data
   */
  def closenessCentrality(edgePropName: Option[String] = None,
                          normalize: Boolean = true): Frame = {
    execute[Frame](ClosenessCentrality(edgePropName, normalize))
  }
}
case class ClosenessCentrality(edgePropName: Option[String] = None,
                               normalize: Boolean = true) extends GraphSummarization[Frame] {

  override def work(state: GraphState): Frame = {
    new Frame(graphframeslib.ClosenessCentrality.run(state.graphFrame, edgePropName, normalize).vertices)
  }
}

