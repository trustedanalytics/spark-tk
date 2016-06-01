from sparktk.lazyloader import implicit
from sparktk.tkcontext import TkContext


def import_hive(hive_query, tc=implicit):
    """
    import data from hive table into frame

    :param hive_query: hive query to fetch data from table
    :return: returns frame with hive table data

    Examples
    --------
    Load data into frame from a hive table based on hive query

    .. code::
    <skip>

        >>> h_query = "select * from demo_test"
        >>> frame = tc.import_hive(h_query)
        -etc-
        >>> frame.inspect()
        [#]  number  strformat
        ======================
        [0]       1  one
        [1]       2  two
        [2]       3  three
        [3]       4  four
    </skip>

    """
    if not isinstance(hive_query, basestring):
        raise ValueError("hive query parameter must be a string, but is {0}.".format(type(hive_query)))
    if tc is implicit:
        implicit.error("tc")
    if not isinstance(tc, TkContext):
        raise ValueError("tc must be type TkContext, received %s" % type(tc))

    scala_frame = tc.sc._jvm.org.trustedanalytics.sparktk.frame.internal.constructors.Import.importHive(tc.jutils.get_scala_sc(), hive_query)

    from sparktk.frame.frame import Frame
    return Frame(tc, scala_frame)
