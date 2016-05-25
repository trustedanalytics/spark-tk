
def export_to_jdbc(self, connection_url, table_name):
    """
    Write current frame to JDBC table

    :param self: frame to export to JDBC table
    :param connection_url: JDBC connection url to database server
    :param table_name: JDBC table name

    Example
    --------
        code snippet:

        - from sparktk import TkContext
        - tc=TkContext(sc)
        - data = [[1, 0.2, -2, 5], [2, 0.4, -1, 6], [3, 0.6, 0, 7], [4, 0.8, 1, 8]]
        - schema = [('a', int), ('b', float),('c', int) ,('d', int)]
        - my_frame = tc.to_frame(data, schema)


        connection_url : (string) : "jdbc:{datasbase_type}://{host}/{database_name}

        Sample connection string for postgres
        ex: jdbc:postgresql://localhost/postgres [standard connection string to connect to default 'postgres' database]

        table_name: (string): table name. It will create new table with given name if it does not exists already.

        - my_frame.export_to_jdbc("jdbc:postgresql://localhost/postgres", "demo_test")

        Verify exported frame in postgres

        From bash shell

        $sudo -su ppostgres psql
        postgres=#\d

        You should see demo_test table.

        Run postgres=#select * from demo_test (to verify frame).

    """
    self._scala.exportToJdbc(connection_url, table_name)