def export_to_orientdb(self, batch_size, db_url, user_name, password, root_password,vertex_type_column_name=None, edge_type_column_name=None):

    """

    Parameters
    ----------

    :param db_url: OrientDB URI
    :param user_name: the database username
    :param password: the database password
    :param root_password: OrientDB server password
    :param vertex_type_column_name: column name from the vertex data frame specified to be the vertex type
    :param edge_type_column_name: column name from the edge data frame specified to be the edge type

    Example
    -------

        >>> from graphframes import examples

        >>> gf = examples.Graphs(tc.sql_context).friends()

        >>> v = tc.frame.create([("a", "Alice", 34,"F"),
        ...     ("b", "Bob", 36,"M"),
        ...     ("c", "Charlie", 30,"M"),
        ...     ("d", "David", 29,"M"),
        ...     ("e", "Esther", 32,"F"),
        ...     ("f", "Fanny", 36,"F"),
        ...     ], ["id", "name", "age","gender"])

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

  <skip>
        >>> db = "test_db"

        >>> g2.export_to_orientdb(batch_size = 1000,db_url="remote:hostname:2424/%s" % db,user_name= "admin",password = "admin",root_password = "orientdb_server_root_password",vertex_type_column_name= "gender",edge_type_column_name="relationship")
  </skip>
    """
    self._scala.exportToOrientdb(batch_size, db_url, user_name, password, root_password,self._tc._jutils.convert.to_scala_option(vertex_type_column_name),self._tc._jutils.convert.to_scala_option(edge_type_column_name))
