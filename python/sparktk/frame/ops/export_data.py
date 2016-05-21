
def export_to_jdbc(self, connection_url, table_name):
    """
    Write current frame to JDBC table

    :param self: frame to export to JDBC table
    :param connection_url: JDBC connection url to connection to postgres server
    :param table_name: JDBC table name

    """
    self._scala.exportToJdbc(connection_url, table_name)