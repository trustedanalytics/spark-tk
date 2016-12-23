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


def betweenness_centrality(self, weight=None):
    """
    **Betweenness Centrality**

    Calculates the betweenness centrality exactly, with an optional weights parameter
    for the distance between the vertices.


    Parameters
    ----------

    :param weight: (Optional String) string name of the weight parameters. If none supplied a uniform weight is assumed

    :return: (Frame) Frame containing the vertex id's an their betweenness centrality value

    Examples
    --------

        >>> vertex_schema = [('id', int)]
        >>> edge_schema = [('src', int), ('dst', int)]

        >>> vertex_rows = [ [1], [2], [3], [4], [5] ]
        >>> edge_rows = [ [1, 2], [1, 3], [2, 3], [1, 4], [4, 5] ]
        >>> vertex_frame = tc.frame.create(vertex_rows, vertex_schema)
        >>> edge_frame = tc.frame.create(edge_rows, edge_schema)

        >>> graph = tc.graph.create(vertex_frame, edge_frame)

        >>> result = graph.betweenness_centrality()
        >>> result.inspect() 
        [#]  id  betweenness_centrality
        ===============================
        [0]   1          0.666666666667
        [1]   2                     0.0
        [2]   3                     0.0
        [3]   4                     0.5
        [4]   5                     0.0



    """
    from sparktk.frame.frame import Frame
    return Frame(self._tc, self._scala.betweennessCentrality(self._tc.jutils.convert.to_scala_option(weight)))
