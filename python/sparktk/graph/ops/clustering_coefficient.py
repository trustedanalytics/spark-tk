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


def clustering_coefficient(self):
    """
    The clustering coefficient of a vertex provides a measure of how
    tightly clustered that vertex's neighborhood is.
    
    Formally:
    
    .. math::
    
       cc(v)  = \frac{ \| \{ (u,v,w) \in V^3: \ \{u,v\}, \{u, w\}, \{v,w \} \in \
           E \} \| }{\| \{ (u,v,w) \in V^3: \ \{v, u \}, \{v, w\} \in E \} \|}
    
    For further reading on clustering
    coefficients, see http://en.wikipedia.org/wiki/Clustering_coefficient.
    
    This method returns a frame with the vertex id associated with it's local
    clustering coefficient

    Parameters
    ----------

    :return: (Frame) Frame containing the vertex id's and their clustering coefficient

    Examples
    --------

        >>> vertex_schema = [('id', int)]
        >>> edge_schema = [('src', int), ('dst', int)]

        >>> vertex_rows = [ [1], [2], [3], [4], [5] ]
        >>> edge_rows = [ [1, 2], [1, 3], [2, 3], [1, 4], [4, 5] ]
        >>> vertex_frame = tc.frame.create(vertex_rows, vertex_schema)
        >>> edge_frame = tc.frame.create(edge_rows, edge_schema)

        >>> graph = tc.graph.create(vertex_frame, edge_frame)

        >>> result = graph.clustering_coefficient()
        >>> result.inspect()
        [#]  id  clustering_coefficient
        ===============================
        [0]   1          0.333333333333
        [1]   2                     1.0
        [2]   3                     1.0
        [3]   4                     0.0
        [4]   5                     0.0

    """
    from sparktk.frame.frame import Frame
    return Frame(self._tc, self._scala.clusteringCoefficient())
