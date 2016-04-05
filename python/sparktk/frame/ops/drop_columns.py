from sparktk.frame.pyframe import PythonFrame
from sparktk.frame.schema import get_indices_for_selected_columns


def drop_columns(self, columns):
    """
    Drops columns from the frame

    :param columns: names of the columns to drop


    Examples
    --------
    For this example, the Frame object *my_frame* accesses a frame with 4 columns
    columns *column_a*, *column_b*, *column_c* and *column_d* and drops 2 columns *column_b* and *column_d* using drop columns.


        <hide>
        >>> sc=[("column_a", str), ("column_b", int), ("column_c", str), ("column_d", int)]
        >>> rows = [["Alameda", 1, "CA", 7], ["Princeton", 2, "NJ", 6], ["NewYork", 3 , "NY", 9]]
        >>> frame = tc.to_frame(rows, sc)
        -etc-

        </hide>

        >>> print frame.schema
        [('column_a', <type 'str'>), ('column_b', <type 'int'>), ('column_c', <type 'str'>), ('column_d', <type 'int'>)]


    Eliminate columns *column_b* and *column_d*:

        >>> frame.drop_columns(["column_b", "column_d"])
        >>> print frame.schema
        [('column_a', <type 'str'>), ('column_c', <type 'str'>)]

    Now the frame only has the columns *column_a* and *column_c*.
    For further examples, see: ref:`example_frame.drop_columns`.
    """
    if isinstance(columns, basestring):
        columns = [columns]
    if self._is_scala:
        self._scala.dropColumns(self._tc.jutils.convert.to_scala_vector_string(columns))
    else:
        victim_indices = get_indices_for_selected_columns(self.schema, columns)
        survivor_indices = [i for i in xrange(len(self.schema)) if i not in victim_indices]
        filtered_schema = [self.schema[i] for i in survivor_indices]

        def filter_fields(row):
            return [row[i] for i in survivor_indices]
        filtered_rdd = self.rdd.map(filter_fields)
        self._frame = PythonFrame(filtered_rdd, filtered_schema)
