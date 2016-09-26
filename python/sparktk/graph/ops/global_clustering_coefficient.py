
def global_clustering_coefficient(self):
    """

    The clustering coefficient of a graph provides a measure of how tightly
    clustered an undirected graph is.

    More formally:

    .. math::

        cc(G)  = \frac{ \| \{ (u,v,w) \in V^3: \ \{u,v\}, \{u, w\}, \{v,w \} \in \
            E \} \| }{\| \{ (u,v,w) \in V^3: \ \{u,v\}, \{u, w\} \in E \} \|}


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
