
def triangle_count(self):
    """
    Counts the number of triangles each vertex is a part of

    Parameters
    ----------

    :return: (Frame) Frame containing the vertex id's and the count of the number of triangle they are in

    Examples
    --------

        >>> vertex_schema = [('id', int)]
        >>> edge_schema = [('src', int), ('dst', int)]

        >>> vertex_rows = [ [1], [2], [3], [4], [5] ]
        >>> edge_rows = [ [1, 2], [1, 3], [2, 3], [1, 4], [4, 5] ]
        >>> vertex_frame = tc.frame.create(vertex_rows, vertex_schema)
        >>> edge_frame = tc.frame.create(edge_rows, edge_schema)

        >>> graph = tc.graph.create(vertex_frame, edge_frame)

        >>> result = graph.triangle_count()
        >>> result.inspect()
        [#]  Triangles  Vertex
        ======================
        [0]          1       1
        [1]          1       2
        [2]          1       3
        [3]          0       4
        [4]          0       5


    """
    from sparktk.frame.frame import Frame
    return Frame(self._tc, self._scala.triangleCount())
