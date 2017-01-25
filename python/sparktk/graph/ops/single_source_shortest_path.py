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


def single_source_shortest_path(self, src_vertex_id, edge_weight=None, max_path_length=None):
    """

    Computes the Single Source Shortest Path (SSSP) for the graph starting from the given vertex ID to every vertex
    in the graph. The algorithm returns the shortest path from the source vertex and the corresponding cost.
    This implementation utilizes a distributed version of Dijkstra's shortest path algorithm.
    Some optional parameters, e.g., maximum path length, constrain the computations for large graphs.

    Parameters
    ----------

    :param src_vertex_id: (any) The source vertex ID
    :param edge_weight: (Optional(str)) The name of the column containing the edge weights,
           If none, every edge is assigned a weight of 1.
    :param max_path_length: (Optional(float)) The maximum path length or cost to limit the SSSP computations.

    :return: (Frame) Frame containing the target vertexIDs, their properties, the shortest path from the source vertex
             and the corresponding cost.


    Examples
    --------

        >>> v = tc.frame.create([("a", "Alice", 34, "female"),
        ...     ("b", "Bob", 36, "male"),
        ...     ("c", "Charlie", 30, "male"),
        ...     ("d", "David", 29, "male"),
        ...     ("e", "Esther", 32, "female"),
        ...     ("f", "Fanny", 36, "female")
        ...     ], ["id", "name", "age", "gender"])

        >>> e = tc.frame.create([("a", "b", "friend", 3),
        ...     ("b", "c", "follow", 12),
        ...     ("c", "b", "follow", 2),
        ...     ("f", "c", "follow", 5),
        ...     ("e", "f", "follow", 4),
        ...     ("e", "d", "friend", 8),
        ...     ("d", "a", "friend", 9),
        ...     ("a", "e", "friend",10)
        ...     ], ["src", "dst", "relationship","distance"])

        >>> graph = tc.graph.create(v, e)

        >>> result = graph.single_source_shortest_path("a")

        >>> result.inspect()
        [#]  id  name     age  gender  cost  path
        ==============================================
        [0]  a   Alice     34  female   0.0  [a]
        [1]  d   David     29  male     2.0  [a, e, d]
        [2]  b   Bob       36  male     1.0  [a, b]
        [3]  e   Esther    32  female   1.0  [a, e]
        [4]  c   Charlie   30  male     2.0  [a, b, c]
        [5]  f   Fanny     36  female   2.0  [a, e, f]

    """
    from sparktk.frame.frame import Frame
    return Frame(self._tc,
                 self._scala.singleSourceShortestPath(src_vertex_id,
                                                      self._tc.jutils.convert.to_scala_option(edge_weight),
                                                      self._tc.jutils.convert.to_scala_option(max_path_length)))
