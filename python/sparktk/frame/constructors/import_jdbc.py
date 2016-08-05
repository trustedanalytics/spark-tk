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
    """
    if not isinstance(connection_url, basestring):
        raise ValueError("connection url parameter must be a string, but is {0}.".format(type(connection_url)))
    if not isinstance(table_name, basestring):
        raise ValueError("table name parameter must be a string, but is {0}.".format(type(table_name)))
    TkContext.validate(tc)

    scala_frame = tc.sc._jvm.org.trustedanalytics.sparktk.frame.internal.constructors.Import.importJdbc(tc.jutils.get_scala_sc(), connection_url, table_name)

    from sparktk.frame.frame import Frame
    return Frame(tc, scala_frame)
