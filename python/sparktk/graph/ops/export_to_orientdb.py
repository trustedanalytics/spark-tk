# vim: set encoding=utf-8

#  Copyright (c) 2016 Intel Corporation 
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

from sparktk.propobj import PropertiesObject
from sparktk.tkcontext import TkContext
tc = TkContext.implicit


def export_to_orientdb(self,
                       orientdb_conf,
                       db_name,
                       vertex_type_column_name=None,
                       edge_type_column_name=None,
                       batch_size=1000,
                       db_properties=None):

    """
    Export Spark-tk Graph (GraphFrame) to OrientDB API creates OrientDB database with the given database name, URL
    and credentials. It exports the vertex and edge dataframes schema OrientDB based on the given vertex_type_column_name
    and edge_type_column_name. If any of them was None it exports it to the base type class.

    Parameters
    ----------

    :param orientdb_conf: (OrientConf) configuration settings for the OrientDB connection
    :param db_name: (str) OrientDB database name
    :param vertex_type_column_name: (Optional(str)) column name from the vertex data frame specified to be the vertex type
    :param edge_type_column_name: (Optional(str)) column name from the edge data frame specified to be the edge type
    :param batch_size: (int) batch size for graph ETL to OrientDB database
    :param db_properties: (Optional(dict(str,any))) additional properties for OrientDB database, for more OrientDB
                            database properties options. See http://orientdb.com/docs/2.1/Configuration.html


    Example
    -------
  <skip>
        >>> v = tc.frame.create([("a", "Alice", 34, "F"),
        ...     ("b", "Bob", 36, "M"),
        ...     ("c", "Charlie", 30, "M"),
        ...     ("d", "David", 29, "M"),
        ...     ("e", "Esther", 32, "F"),
        ...     ("f", "Fanny", 36, "F"),
        ...     ], ["id", "name", "age", "gender"])

        >>> e = tc.frame.create([("a", "b", "friend"),
        ...     ("b", "c", "follow"),
        ...     ("c", "b", "follow"),
        ...     ("f", "c", "follow"),
        ...     ("e", "f", "follow"),
        ...     ("e", "d", "friend"),
        ...     ("d", "a", "friend"),
        ...     ("a", "e", "friend")
        ...     ], ["src", "dst", "relationship"])

        >>> sparktk_graph = tc.graph.create(v, e)

        >>> hostname = "localhost"

        >>> port_number = "2424"

        >>> db_name = "GraphDatabase"

        >>> root_password = "root"

        >>> orient_conf = tc.graph.create_orientdb_config(hostname, port_number, "admin", "admin", root_password)

        >>> result = sparktk_graph.export_to_orientdb(orient_conf,
        ...                                           db_name,
        ...                                           vertex_type_column_name="gender",
        ...                                           edge_type_column_name="relationship",
        ...                                           batch_size = 1000,
        ...                                           db_properties = ({"db.validation":"false"}))

        >>> result
        db_uri                    = remote:hostname:2424/test_db
        edge_types                = {u'follow': 4L, u'friend': 4L}
        exported_edges_summary    = {u'Total Exported Edges Count': 8L, u'Failure Count': 0L}
        exported_vertices_summary = {u'Total Exported Vertices Count': 6L, u'Failure Count': 0L}
        vertex_types              = {u'M': 3L, u'F': 3L}
  </skip>
    """
    return ExportOrientdbStats(self._tc,
                               self._scala.exportToOrientdb(orientdb_conf._scala,
                                                               db_name,
                                                               self._tc._jutils.convert.to_scala_option(vertex_type_column_name),
                                                               self._tc._jutils.convert.to_scala_option(edge_type_column_name),
                                                               batch_size,
                                                               self._tc._jutils.convert.to_scala_option_map(db_properties)))


class ExportOrientdbStats(PropertiesObject):
    """
    holds the data returned from exporting a graphframe to OrientDB database
    """
    def __init__(self, tc, scala_result):
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
