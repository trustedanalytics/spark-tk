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


def import_jdbc(connection_url, table_name, tc=TkContext.implicit):
    """
    Import data from jdbc table into frame.

    Parameters
    ----------

    :param connection_url: (str) JDBC connection url to database server
    :param table_name: (str) JDBC table name
    :return: (Frame) returns frame with jdbc table data

    Examples
    --------
    Load a frame from a jdbc table specifying the connection url to the database server.

    <skip>
        >>> url = "jdbc:postgresql://localhost/postgres"
        >>> tb_name = "demo_test"

        >>> frame = tc.frame.import_jdbc(url, tb_name)
        -etc-

        >>> frame.inspect()
        [#]  a  b    c   d
        ==================
        [0]  1  0.2  -2  5
        [1]  2  0.4  -1  6
        [2]  3  0.6   0  7
        [3]  4  0.8   1  8

        >>> frame.schema
        [(u'a', int), (u'b', float), (u'c', int), (u'd', int)]
    </skip>

    Notes
    -----

        java.sql.SQLException: No suitable driver found for <jdbcUrl>

    If this error is encountered while running your application, then your JDBC library cannot be found by the node
    running the application. If you're running in Local mode, make sure that you have used the --driver-class-path
    parameter. If a Spark cluster is involved, make sure that each cluster member has a copy of library, and that
    each node of the cluster has been restarted since you modified the spark-defaults.conf file.  See this
    [site](https://sparkour.urizone.net/recipes/using-jdbc/).

    Sparktk does not come with any JDBC drivers.  A driver compatible with the JDBC data source must be supplied when
    creating the TkContext instance:

        <skip>
        >>> tc = sparktk.TkContext(pyspark_submit_args='--jars myJDBCDriver.jar')
        </skip>

    """
    if not isinstance(connection_url, basestring):
        raise ValueError("connection url parameter must be a string, but is {0}.".format(type(connection_url)))
    if not isinstance(table_name, basestring):
        raise ValueError("table name parameter must be a string, but is {0}.".format(type(table_name)))
    TkContext.validate(tc)

    scala_frame = tc.sc._jvm.org.trustedanalytics.sparktk.frame.internal.constructors.Import.importJdbc(tc.jutils.get_scala_sc(), connection_url, table_name)

    from sparktk.frame.frame import Frame
    return Frame(tc, scala_frame)
