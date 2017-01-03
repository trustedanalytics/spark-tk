# vim: set encoding=utf-8

#  Copyright (c) 2016 Intel Corporation 
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

from sparktk.tkcontext import TkContext
tc = TkContext.implicit


def closeness_centrality(self, edge_prop_name=None, normalized=True):
    """

     Compute closeness centrality for nodes.
     Closeness centrality of a node is the reciprocal of the sum of the shortest path distances from this node to all
     other nodes in the graph. Since the sum of distances depends on the number of nodes in the
     graph, closeness is normalized by the sum of minimum possible distances.
     In the case of disconnected graph, the algorithm computes the closeness centrality for each connected part.
     If the edge weight is considered then the shortest-path length will be computed using Dijkstra's algorithm with
     that edge weight.

     Reference: Linton C. Freeman: Centrality in networks: I.Conceptual clarification. Social Networks 1:215-239, 1979.
     http://leonidzhukov.ru/hse/2013/socialnetworks/papers/freeman79-centrality.pdf

    Parameters
    ----------

    :param edge_prop_name: (Optional(str)) optional edge column name to be used as edge weight
    :param normalized: (boolean) normalizes the closeness centrality value to the number of nodes connected to it
    divided by the rest number of nodes in the graph, this is effective in the case of disconnected graph

    :return: (dict) dictionary of the graph vertex IDs and each corresponding closeness centrality value


    Examples
    --------


        >>> v = tc.frame.create([(1, "Alice"),
        ...     (2, "Bob"),
        ...     (3, "Charlie"),
        ...     (4, "David"),
        ...     (5, "Esther"),
        ...     (6, "Fanny")], ["id", "name"])

        >>> e = tc.frame.create([(1,2, 3),
        ...     (2,3, 12),
        ...     (3,4, 2),
        ...     (3,5, 5),
        ...     (4,5, 4),
        ...     (4,6, 8),
        ...     (5,6, 9)], ["src", "dst", "distance"])

        >>> graph = tc.graph.create(v, e)

        >>> result = graph.closeness_centrality(edge_prop_name="distance", normalized=False)

        >>> result
        {5L: 0.1111111111111111,
        1L: 0.0625,
        6L: 0.0,
        2L: 0.06153846153846154,
        3L: 0.17647058823529413,
        4L: 0.16666666666666666}

    """
    to_python_dict = self._tc.jutils.convert.scala_map_to_python
    return to_python_dict(self._scala.closenessCentrality(self._tc.jutils.convert.to_scala_option(edge_prop_name),
                                                          normalized))
