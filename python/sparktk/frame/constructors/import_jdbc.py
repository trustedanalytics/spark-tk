from sparktk.lazyloader import implicit
from sparktk.tkcontext import TkContext


def import_jdbc(connection_url, table_name, tc=implicit):
    """
    import data from jdbc table into frame

    :param connection_url: JDBC connection url to database server
    :param table_name: JDBC table name
    :return: returns frame with jdbc table data

    Examples
    --------
    Load a frame from a jdbc table specifying the connection url to the database server.

    .. code::

        >>> url = "jdbc:postgresql://localhost/postgres"
        >>> tb_name = "demo_test"
        >>> frame = tc.import_jdbc(url, tb_name)
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

    """
    if not isinstance(connection_url, basestring):
        raise ValueError("connection url parameter must be a string, but is {0}.".format(type(connection_url)))
    if not isinstance(table_name, basestring):
        raise ValueError("table name parameter must be a string, but is {0}.".format(type(table_name)))
    if tc is implicit:
        implicit.error("tc")
    if not isinstance(tc, TkContext):
        raise ValueError("tc must be type TkContext, received %s" % type(tc))

    scala_frame = tc.sc._jvm.org.trustedanalytics.sparktk.frame.internal.constructors.Import.importJdbc(tc.jutils.get_scala_sc(), connection_url, table_name)

    from sparktk.frame.frame import Frame
    return Frame(tc, scala_frame)
