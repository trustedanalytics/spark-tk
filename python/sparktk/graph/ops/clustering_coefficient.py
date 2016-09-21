
def clustering_coefficient(self):
    """
    Calculates the local clustering coefficient of every vertex

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
        [#]  Vertex  Clustering_Coefficient
        ===================================
        [0]       1          0.333333333333
        [1]       2                     1.0
        [2]       3                     1.0
        [3]       4                     0.0
        [4]       5                     0.0

    """
    from sparktk.frame.frame import Frame
    return Frame(self._tc, self._scala.clusteringCoefficient())
