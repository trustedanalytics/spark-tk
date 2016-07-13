from sparktk.lazyloader import implicit


def create(vertex_frame, edge_frame, tc=implicit):
    """
    Create a sparktk Graph from two sparktk Frames

    Parameters
    ----------
    :param vertex_frame: frame defining the vertices for the graph; schema must have a column named "id" which provides
                         unique vertex ID.  All other columns are treated as vertex properties.  If a column is also
                         found named "vertex", it will be used as a special label to denote the type of vertex,
                         for example, when interfacing with logic (such as a graph DB) which expects a specific vertex type
    :param edge_frame: frame defining the edges of the graph; schema must have columns names "src" and "dst" which
                       provide the vertex ids of the edge.  All other columns are treated as edge properties.  If a
                       column is also found named "edge", it will be used as a special label to denote the type of edge,
                       for example, when interfacing with logic (such as a graph DB) which expects a specific edge type
    """
    if tc is implicit:
        implicit.error('tc')

    from sparktk.frame.frame import Frame
    from sparktk.graph.graph import Graph

    # do some python validate before going to scala
    if not isinstance(vertex_frame, Frame):
        raise TypeError("vertex_frame must be of type Frame, received %s" % type(vertex_frame))
    if 'id' not in vertex_frame.column_names:
        raise ValueError("vertex_frame must contain column 'id', found %s" % vertex_frame.column_names)
    if not isinstance(edge_frame, Frame):
        raise TypeError("edge_frame must be of type Frame, received %s" % type(edge_frame))
    if 'src' not in edge_frame.column_names:
        raise ValueError("edge_frame must contain column 'src', found %s" % edge_frame.column_names)
    if 'dst' not in edge_frame.column_names:
        raise ValueError("edge_frame must contain column 'dst', found %s" % edge_frame.column_names)

    scala_vertex_frame = vertex_frame._scala
    scala_edge_frame = edge_frame._scala
    scala_graph = Graph.create_scala_graph_from_scala_frames(tc, scala_vertex_frame, scala_edge_frame)
    return Graph(tc, scala_graph)


def create_from_spark_graphframe(graphframe, tc=implicit):
    """Create a sparktk Graph from a Spark GraphFrame"""
    if tc is implicit:
        implicit.error('tc')
    from sparktk.graph.graph import Graph
    return Graph(tc, graphframe)


