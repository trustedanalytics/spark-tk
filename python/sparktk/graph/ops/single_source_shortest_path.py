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


def single_source_shortest_path(self, src_vertex_id, edge_prop_name, max_path_length):
    """

    Computes the Single Source Shortest Paths (SSSP)for the given graph starting from the given vertex ID,
    returns the target vertexID, he shortest path from the source vertex and the corresponding cost.
    this implementation utilizes a distributed version of Dijkstra-based shortest path algorithm.
    Some optional parameters, e.g., maximum path length, are provided to constraint the computations for large graphs.

    Parameters
    ----------

    :param src_vertex_id: (Any) source vertex ID
    :param edge_prop_name: (Optional(str)) optional edge column name to be used as edge weight
    :param max_path_length (Optional(double))optional maximum path length or cost to limit the SSSP computations
    :return: (Frame) the target vertexID, he shortest path from the source vertex and the corresponding cost


    Examples
    --------

         >>> v = tc.frame.create([("a", "Alice", 34, "F"),
        ...     ("b", "Bob", 36, "M"),
        ...     ("c", "Charlie", 30, "M"),
        ...     ("d", "David", 29, "M"),
        ...     ("e", "Esther", 32, "F"),
        ...     ("f", "Fanny", 36, "F"),
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
<skip>
        >>> result = graph.single_source_shortest_path("a")

         >>> result.inspect() 
</skip>

    """
    from sparktk.frame.frame import Frame
    return Frame(self._tc, self._scala.single_source_shortest_path(self,
                                                                   src_vertex_id,
                                                                   self._tc._jutils.convert.to_scala_option(edge_prop_name),
                                                                   self._tc._jutils.convert.to_scala_option(max_path_length)))
