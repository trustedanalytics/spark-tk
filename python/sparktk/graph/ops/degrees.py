
def degrees(self, degree_option='undirected'):
    """
    **Degree Calculation**

    A fundamental quantity in graph analysis is the degree of a vertex:
    The degree of a vertex is the number of edges adjacent to it.

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

        >>> result = graph.degrees(degree_option="out")
        >>> result.inspect() 
        [#]  Vertex  Degree
        ===================
        [0]       1       3
        [1]       2       1
        [2]       3       0
        [3]       4       1
        [4]       5       0


        >>> result = graph.degrees(degree_option="in")
        >>> result.inspect()
        [#]  Vertex  Degree
        ===================
        [0]       1       0
        [1]       2       1
        [2]       3       2
        [3]       4       1
        [4]       5       1

    """
    from sparktk.frame.frame import Frame
    return Frame(self._tc, self._scala.degree(degree_option))
