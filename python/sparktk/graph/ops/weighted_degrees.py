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


def weighted_degrees(self, edge_weight, degree_option='undirected', default_weight=0.0):
    """
    **Degree Calculation**

    A fundamental quantity in graph analysis is the degree of a vertex:
    The degree of a vertex is the number of edges adjacent to it.

    For a directed edge relation, a vertex has both an out-degree (the number of
    edges leaving the vertex) and an in-degree (the number of edges entering the
    vertex).

    In the presence of edge weights, vertices can have weighted degrees: The
    weighted degree of a vertex is the sum of weights of edges adjacent to it.
    Analogously, the weighted in-degree of a vertex is the sum of the weights of
    the edges entering it, and the weighted out-degree is the sum
    of the weights of the edges leaving the vertex. If a property is missing or
    empty on particular vertex, the default weight is used.

    Parameters
    ----------

    :param edge_weight: (String) Name of the property that contains and edge weight
    :param degree_option: (String) Either in, out or undirected. String describing the direction of edges
    :param default_weight: (Numeric) Default weight value if a vertex has no value for the edge weight property

    :return: (Frame) Frame containing the vertex id's an their weights

    Examples
    --------

        >>> vertex_schema = [('id', int), ('label', float)]
        >>> edge_schema = [('src', int), ('dst', int), ('weight', int)]

        >>> vertex_rows = [ [1, 1], [2, 1], [3, 5], [4, 5], [5, 5] ]
        >>> edge_rows = [ [1, 2, 2], [1, 3, 1], [2, 3, 1], [1, 4, 1], [4, 5, 1] ]
        >>> vertex_frame = tc.frame.create(vertex_rows, vertex_schema)
        >>> edge_frame = tc.frame.create(edge_rows, edge_schema)

        >>> graph = tc.graph.create(vertex_frame, edge_frame)

        >>> result = graph.weighted_degrees(edge_weight="weight", degree_option="out")
        >>> result.inspect() 
        [#]  id  degree
        ===============
        [0]   1       4
        [1]   2       1
        [2]   3       0
        [3]   4       1
        [4]   5       0


        >>> result = graph.weighted_degrees(edge_weight="weight", degree_option="in")
        >>> result.inspect()
        [#]  id  degree
        ===============
        [0]   1       0
        [1]   2       2
        [2]   3       2
        [3]   4       1
        [4]   5       1

    """
    from sparktk.frame.frame import Frame
    return Frame(self._tc, self._scala.weightedDegree(edge_weight, degree_option, default_weight))
