from sparktk.frame.ops.take import TakeCollectHelper
from sparktk.frame.schema import get_schema_for_columns
from sparktk.arguments import affirm_type

def collect(self, columns=None):
    """
    Brings all the rows of data from the frame into a local python list of lists

    (Use the 'take' operation for control over row count and offset of the collected data)

    Parameters
    ----------

    :param columns: (Optional[str or List[str]) If not None, only the given columns' data will be provided.
                    By default, all columns are included.
    :return: (List[List[*]]) the frame data represented as a list of lists

    Examples
    --------

        >>> schema = [('name',str), ('age', int), ('tenure', int), ('phone', str)]
        >>> rows = [['Fred', 39, 16, '555-1234'], ['Susan', 33, 3, '555-0202'], ['Thurston', 65, 26, '555-4510'], ['Judy', 44, 14, '555-2183']]
        >>> frame = tc.frame.create(rows, schema)
        >>> frame.collect()
        [['Fred', 39, 16, '555-1234'], ['Susan', 33, 3, '555-0202'], ['Thurston', 65, 26, '555-4510'], ['Judy', 44, 14, '555-2183']]

        >>> frame.collect(['name', 'phone'])
        [['Fred', '555-1234'], ['Susan', '555-0202'], ['Thurston', '555-4510'], ['Judy', '555-2183']]

        <hide>
        >>> tmp = frame._scala
        >>> frame.collect(['name', 'phone'])
        [[u'Fred', u'555-1234'], [u'Susan', u'555-0202'], [u'Thurston', u'555-4510'], [u'Judy', u'555-2183']]

        </hide>

    """
    if columns is not None:
        affirm_type.list_of_str(columns, "columns")
        if not columns:
            return []
    if self._is_scala:
        scala_data = self._scala.collect(self._tc.jutils.convert.to_scala_option_list_string(columns))
        schema = get_schema_for_columns(self.schema, columns) if columns else self.schema
        data = TakeCollectHelper.scala_rows_to_python(self._tc, scala_data, schema)
    else:
        if columns:
            select = TakeCollectHelper.get_select_columns_function(self.schema, columns)
            data = self._python.rdd.map(select).collect()
        else:
            data = self._python.rdd.collect()
    return data
