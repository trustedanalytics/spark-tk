from sparktk.propobj import PropertiesObject


def export_to_orientdb(self, db_url, user_name, password, root_password,vertex_type_column_name=None, edge_type_column_name=None,batch_size=1000):

    """
    Export Spark-tk Graph (GraphFrame) to OrientDB API creates OrientDB database with the given database name, URL
    and credentials. It exports the vertex and edge dataframes schema OrientDB based on the given vertex_type_column_name
    and edge_type_column_name. If any of them was None it exports it to the base type class.

    Parameters
    ----------

    :param:(str) db_url: OrientDB URI
    :param:(str) user_name: the database username
    :param:(str) password: the database password
    :param:(str) root_password: OrientDB server password
    :param:(Optional(str)) vertex_type_column_name: column name from the vertex data frame specified to be the vertex type
    :param:(Optional(str)) edge_type_column_name: column name from the edge data frame specified to be the edge type
    :param:(int) batch_size: batch size for graph ETL to OrientDB database

    Example
    -------

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

        >>> sparktk_graph = tc.graph.create(v,e)

  <skip>
        >>> db = "test_db"

        >>> result = sparktk_graph.export_to_orientdb(db_url="remote:hostname:2424/%s" % db,user_name= "admin",password = "admin",root_password = "orientdb_server_root_password",vertex_type_column_name= "gender",edge_type_column_name="relationship")

        >>> result
        db_uri                    = remote:hostname:2424/test_db
        edge_types                = {u'follow': 4L, u'friend': 4L}
        exported_edges_summary    = {u'Total Exported Edges Count': 8L, u'Failure Count': 0L}
        exported_vertices_summary = {u'Total Exported Vertices Count': 6L, u'Failure Count': 0L}
        vertex_types              = {u'M': 3L, u'F': 3L}
  </skip>
    """
    return ExportToOrientdbReturn(self._tc,self._scala.exportToOrientdb(db_url, user_name, password, root_password,self._tc._jutils.convert.to_scala_option(vertex_type_column_name),self._tc._jutils.convert.to_scala_option(edge_type_column_name), batch_size))


class ExportToOrientdbReturn(PropertiesObject):
    """
    ExportToOrientdbReturn holds the data returned from ExportToOrientDB
    """
    def __init__(self, tc,scala_result):
        self._tc = tc
        self._exported_vertices_summary = self._tc.jutils.convert.scala_map_to_python(scala_result.exportedVerticesSummary())
        self._vertices_types = self._tc.jutils.convert.scala_map_to_python(scala_result.verticesTypes())
        self._exported_edges_summary = self._tc.jutils.convert.scala_map_to_python(scala_result.exportedEdgesSummary())
        self._edges_types = self._tc.jutils.convert.scala_map_to_python(scala_result.edgesTypes())
        self._db_uri = scala_result.dbUri()

    @property
    def exported_vertices_summary(self):
        """A dictionary for the exported vertices"""
        return self._exported_vertices_summary

    @property
    def vertex_types(self):
        """A dictionary for the exported vertex types and count"""
        return self._vertices_types

    @property
    def exported_edges_summary(self):
        """A dictionary for the exported edges"""
        return self._exported_edges_summary

    @property
    def edge_types(self):
        """A dictionary for the exported edge types and count"""
        return self._edges_types

    @property
    def db_uri(self):
        """OrientDB database URL"""
        return self._db_uri
