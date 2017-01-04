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


def degree_centrality(self, degree_option='undirected'):
    """
    **Degree Centrality Calculation**

    A fundamental quantity in graph analysis is the degree centrality of a vertex:
    The degree of a vertex is the number of edges adjacent to it, normalized against the
    highest possible degree (number of vertices in graph - 1).

    For a directed edge relation, a vertex has both an out-degree (the number of
    edges leaving the vertex) and an in-degree (the number of edges entering the
    vertex).

    Parameters
    ----------

    :param degree_option: (String) Either in, out or undirected. String describing the direction of edges

    :return: (Frame) Frame containing the vertex id's an their weights

    Examples
    --------

        >>> vertex_schema = [('id', int)]
        >>> edge_schema = [('src', int), ('dst', int)]

        >>> vertex_rows = [ [1], [2], [3], [4], [5] ]
        >>> edge_rows = [ [1, 2], [1, 3], [2, 3], [1, 4], [4, 5] ]
        >>> vertex_frame = tc.frame.create(vertex_rows, vertex_schema)
        >>> edge_frame = tc.frame.create(edge_rows, edge_schema)

        >>> graph = tc.graph.create(vertex_frame, edge_frame)

        >>> result = graph.degree_centrality(degree_option="out")
        >>> result.inspect() 
        [#]  id  degree_centrality
        ==========================
        [0]   1               0.75
        [1]   2               0.25
        [2]   3                0.0
        [3]   4               0.25
        [4]   5                0.0



        >>> result = graph.degree_centrality(degree_option="in")
        >>> result.inspect()
        [#]  id  degree_centrality
        ==========================
        [0]   1                0.0
        [1]   2               0.25
        [2]   3                0.5
        [3]   4               0.25
        [4]   5               0.25

    """
    from sparktk.frame.frame import Frame
    return Frame(self._tc, self._scala.degreeCentrality(degree_option))
