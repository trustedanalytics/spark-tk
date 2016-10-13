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

from sparktk.tkcontext import TkContext


def import_orientdb_graph(db_url, user_name, password, root_password,tc=TkContext.implicit):
    """
    Import graph from OrientDB to spark-tk as spark-tk graph (Spark GraphFrame)

    Parameters
    ----------
    :param:(str) db_url: OrientDB URI
    :param:(str) user_name: the database username
    :param:(str) password: the database password
    :param :(str)root_password: OrientDB server password

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

        >>> sparktk_graph.export_to_orientdb(db_url="remote:hostname:2424/%s" % db,user_name= "admin",password = "admin",root_password = "orientdb_server_root_password",vertex_type_column_name= "gender",edge_type_column_name="relationship")

        >>> imported_gf = tc.graph.import_orientdb_graph(db_url="remote:hostname:2424/%s" % db,user_name= "admin",password = "admin",root_password = "orientdb_server_root_password")

        >>> imported_gf.graphframe.vertices.show()

+-------+------+---+---+
|   name|gender| id|age|
+-------+------+---+---+
|    Bob|     M|  b| 36|
|  David|     M|  d| 29|
|Charlie|     M|  c| 30|
|  Alice|     F|  a| 34|
| Esther|     F|  e| 32|
|  Fanny|     F|  f| 36|
+-------+------+---+---+

        >>> imported_gf.graphframe.edges.show()

+---+------------+---+
|dst|relationship|src|
+---+------------+---+
|  f|      follow|  e|
|  b|      follow|  c|
|  c|      follow|  b|
|  c|      follow|  f|
|  b|      friend|  a|
|  a|      friend|  d|
|  d|      friend|  e|
|  e|      friend|  a|
+---+------------+---+

  </skip>
    """
    TkContext.validate(tc)
    scala_graph = tc.sc._jvm.org.trustedanalytics.sparktk.graph.internal.constructors.fromorientdb.ImportFromOrientdb.importOrientdbGraph(tc.jutils.get_scala_sc(), db_url,user_name,password,root_password)
    from sparktk.graph.graph import Graph
    return Graph(tc, scala_graph)
