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

def export_to_jdbc(self, connection_url, table_name):
    """
    Write current frame to JDBC table

    Parameters
    ----------

    :param connection_url: (str) JDBC connection url to database server
    :param table_name: (str) JDBC table name

    Example
    -------

    <skip>

        >>> from sparktk import TkContext
        >>> c=TkContext(sc)
        >>> data = [[1, 0.2, -2, 5], [2, 0.4, -1, 6], [3, 0.6, 0, 7], [4, 0.8, 1, 8]]
        >>> schema = [('a', int), ('b', float),('c', int) ,('d', int)]
        >>> my_frame = tc.frame.create(data, schema)
        <progress>
    </skip>

    connection_url : (string) : "jdbc:{datasbase_type}://{host}/{database_name}

    Sample connection string for postgres
    ex: jdbc:postgresql://localhost/postgres [standard connection string to connect to default 'postgres' database]

    table_name: (string): table name. It will create new table with given name if it does not exists already.

    <skip>
        >>> my_frame.export_to_jdbc("jdbc:postgresql://localhost/postgres", "demo_test")
        <progress>
    </skip>

    Verify exported frame in postgres

        From bash shell

        $sudo -su ppostgres psql
        postgres=#\d

    You should see demo_test table.

    Run postgres=#select * from demo_test (to verify frame).

     Notes
    -----

        java.sql.SQLException: No suitable driver found for <jdbcUrl>

    If this error is encountered while running your application, then your JDBC library cannot be found by the node
    running the application. If you're running in Local mode, make sure that you have used the --driver-class-path
    parameter. If a Spark cluster is involved, make sure that each cluster member has a copy of library, and that
    each node of the cluster has been restarted since you modified the spark-defaults.conf file.  See this
    [site](https://sparkour.urizone.net/recipes/using-jdbc/).

    Sparktk does not come with any JDBC drivers.  A driver compatible with the JDBC data sink must be supplied when
    creating the TkContext instance:

        <skip>
        >>> tc = sparktk.TkContext(pyspark_submit_args='--jars myJDBCDriver.jar')
        </skip>
    """
    self._scala.exportToJdbc(connection_url, table_name)