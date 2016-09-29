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
    Import graph from OrientDB to spark-tk as spark-tk graph (Spark graph frame)

    Parameters
    ----------
    :param db_url: OrientDB URI
    :param user_name: the database username
    :param password: the database password
    :param root_password: OrientDB server password
    """
    TkContext.validate(tc)
    scala_graph = tc.sc._jvm.org.trustedanalytics.sparktk.graph.internal.constructors.fromorientdb.ImportFromOrientdb.importOrientdbGraph(tc.jutils.get_scala_sc(), db_url,user_name,password,root_password)
    from sparktk.graph.graph import Graph
    return Graph(tc, scala_graph)
