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


def vertex_count(self):

    """
    Returns the number of rows in the vertices frame in this graph

    Example
    -------

        >>> from graphframes import examples

        >>> gf = examples.Graphs(tc.sql_context).friends()

        >>> from sparktk.graph.graph import Graph

        >>> g = Graph(tc, gf)

        >>> g.vertex_count()
        6


        >>> v = tc.frame.create([("a", "Alice", 34),
        ...     ("b", "Bob", 36),
        ...     ("c", "Charlie", 30),
        ...     ("d", "David", 29),
        ...     ("e", "Esther", 32),
        ...     ("f", "Fanny", 36),
        ...     ], ["id", "name", "age"])

        >>> e = tc.frame.create([("a", "b", "friend"),
        ...     ("b", "c", "follow"),
        ...     ("c", "b", "follow"),
        ...     ("f", "c", "follow"),
        ...     ("e", "f", "follow"),
        ...     ("e", "d", "friend"),
        ...     ("d", "a", "friend"),
        ...     ("a", "e", "friend")
        ...     ], ["src", "dst", "relationship"])

        >>> g2 = tc.graph.create(v, e)

        >>> g2.vertex_count()
        6

    """
    return int(self._scala.vertexCount())