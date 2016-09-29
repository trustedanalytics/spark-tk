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


def create(source_or_vertices_frame, edges_frame=None, tc=TkContext.implicit):
    """
    Create a sparktk Graph from two sparktk Frames (or some other source)

    Parameters
    ----------
    :param source_or_vertices_frame: a graph source or a vertices frame
                        Valid sources include: a python and spark GraphFrame, or a scala Graph
                        Otherwise if a vertices frame is provided, then the edges_frame arg must also be supplied.
                        A vertices frame defines the vertices for the graph and must have a schema with a column
                        named "id" which provides unique vertex ID.  All other columns are treated as vertex properties.
                        If a column is also found named "vertex_type", it will be used as a special label to denote the
                        type of vertex, for example, when interfacing with logic (such as a graph DB) which expects a
                        specific vertex type.

    :param edges_frame: (valid only if the source_or_vertices_frame arg is a vertices Frame) An edge frame defines the
                        edges of the graph; schema must have columns names "src" and "dst" which provide the vertex ids
                        of the edge.  All other columns are treated as edge properties.  If a column is also found named
                        "edge_type", it will be used as a special label to denote the type of edge, for example, when
                        interfacing with logic (such as a graph DB) which expects a specific edge type.
    """
    TkContext.validate(tc)
    from sparktk.graph.graph import Graph
    return Graph(tc, source_or_vertices_frame, edges_frame)
