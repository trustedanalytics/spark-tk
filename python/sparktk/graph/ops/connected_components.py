
def connected_components(self):
    """

    Connected components determines groups all the vertices in a particular graph
    by whether or not there is path between these vertices. This method returns
    a frame with the vertices and their corresponding component

    Parameters
    ----------

    :return: (Frame) Frame containing the vertex id's and their components

    Examples
    --------

        >>> vertex_schema = [('id', int)]
        >>> edge_schema = [('src', int), ('dst', int)]

        >>> vertex_rows = [ [1], [2], [3], [4], [5] ]
        >>> edge_rows = [ [1, 2], [1, 3], [2, 3], [4, 5] ]
        >>> vertex_frame = tc.frame.create(vertex_rows, vertex_schema)
        >>> edge_frame = tc.frame.create(edge_rows, edge_schema)

        >>> graph = tc.graph.create(vertex_frame, edge_frame)

        >>> result = graph.connected_components()
        >>> result.inspect() 
        [#]  Vertex  Component
        ======================
        [0]       1          1
        [1]       2          1
        [2]       3          1
        [3]       4          4
        [4]       5          4

    """
    from sparktk.frame.frame import Frame
    return Frame(self._tc, self._scala.connectedComponents())
