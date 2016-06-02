from sparktk.lazyloader import implicit
from sparktk.tkcontext import TkContext
from sparktk.dtypes import dtypes

def import_hbase(table_name, schema, start_tag=None, end_tag=None, tc=implicit):
    """
    Import data from hbase table into frame

    :param table_name: hbase table name
    :param schema: hbase schema as a List of List(string) (columnFamily, columnName, dataType for cell value)
    :param start_tag: optional start tag for filtering
    :param end_tag: optional end tag for filtering
    :return: frame with data from hbase table

    Example
    ---------

    Load data into frame from a hbase table

    .. code ::
    <skip>

        >>> frame = tc.import_hbase("demo_test_hbase", [["test_family", "a", int],["test_family", "b", float], ["test_family", "c", int],["test_family", "d", int]])
        -etc-
        >>> frame.inspect()
        [#]  test_family_a  test_family_b  test_family_c  test_family_d
        ===============================================================
        [0]              1            0.2             -2              5
        [1]              2            0.4             -1              6
        [2]              3            0.6              0              7
        [3]              4            0.8              1              8

    </skip>

    """

    if not isinstance(table_name, basestring):
        raise ValueError("table name parameter must be a string, but is {0}.".format(type(table_name)))
    if not isinstance(schema, list):
        raise ValueError("schema parameter must be a list, but is {0}.".format(type(table_name)))
    if tc is implicit:
        implicit.error("tc")
    if not isinstance(tc, TkContext):
        raise ValueError("tc must be type TkContext, received %s" % type(tc))

    inner_lists=[tc._jutils.convert.to_scala_list([item[0], item[1], dtypes.to_string(item[2])]) for item in schema]
    scala_final_schema = tc.jutils.convert.to_scala_list(inner_lists)

    scala_frame = tc.sc._jvm.org.trustedanalytics.sparktk.frame.internal.constructors.Import.importHbase(tc.jutils.get_scala_sc(),
                                                                                                         table_name, scala_final_schema,
                                                                                                         tc._jutils.convert.to_scala_option(start_tag),
                                                                                                         tc._jutils.convert.to_scala_option(end_tag))

    from sparktk.frame.frame import Frame
    return Frame(tc, scala_frame)
