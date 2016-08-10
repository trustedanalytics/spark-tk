
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