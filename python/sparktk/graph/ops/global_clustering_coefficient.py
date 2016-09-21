
def global_clustering_coefficient(self):
    """
    Calculates the global clustering coeffcicient across the entire graph

    Parameters
    ----------

    :return: (Double) The global clustering coeffcicient of the graph

    Examples
    --------

        >>> vertex_schema = [('id', int)]
        >>> edge_schema = [('src', int), ('dst', int)]

        >>> vertex_rows = [ [1], [2], [3], [4], [5] ]
        >>> edge_rows = [ [1, 2], [1, 3], [2, 3], [1, 4], [4, 5] ]
        >>> vertex_frame = tc.frame.create(vertex_rows, vertex_schema)
        >>> edge_frame = tc.frame.create(edge_rows, edge_schema)

        >>> graph = tc.graph.create(vertex_frame, edge_frame)

        >>> graph.global_clustering_coefficient()
        0.5

    """
    return self._scala.globalClusteringCoefficient()
