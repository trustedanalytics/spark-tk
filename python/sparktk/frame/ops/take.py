from collections import namedtuple

import sparktk.frame.schema
from sparktk.dtypes import dtypes
from sparktk.arguments import affirm_type, require_type
from sparktk.frame.schema import get_schema_for_columns


def take(self, n, offset=0, columns=None):
    """
    Get data subset.

    Take a subset of the currently active Frame.

    (See 'collect' operation to simply get all the data from the Frame)

    Parameters
    ----------

    :param n: (int) The number of rows to get from the frame (warning: do not overwhelm the python session
                    by taking too much)
    :param offset: (Optional[int]) The number of rows to skip before starting to copy.
    :param columns: (Optional[str or list[str]) If not None, only the given columns' data will be provided.
                    By default, all columns are included.
    :return: (list[list[data]]) raw frame data

    Examples
    --------

    <hide>
        >>> schema = [('name',str), ('age', int), ('tenure', int), ('phone', str)]
        >>> rows = [['Fred', 39, 16, '555-1234'], ['Susan', 33, 3, '555-0202'], ['Thurston', 65, 26, '555-4510'], ['Judy', 44, 14, '555-2183']]
        >>> frame = tc.frame.create(rows, schema)
        -etc-
    </hide>

    Consider the following frame:
        >>> frame.inspect()
        [#]  name      age  tenure  phone
        ====================================
        [0]  Fred       39      16  555-1234
        [1]  Susan      33       3  555-0202
        [2]  Thurston   65      26  555-4510
        [3]  Judy       44      14  555-2183

    Use take to get the first two rows and look at the schema and data in the result:

        >>> frame.take(2)
        [['Fred', 39, 16, '555-1234'], ['Susan', 33, 3, '555-0202']]

    Limit the columns in our result to just the name and age column:

        >>> frame.take(2, columns=['name', 'age'])
        [['Fred', 39], ['Susan', 33]]

    <hide>
        >>> tmp = frame._scala  # flip over to scala and try
        >>> frame.take(2, columns=['name', 'age'])
        [[u'Fred', 39], [u'Susan', 33]]

    </hide>

    """
    require_type.non_negative_int(n, "n")
    require_type.non_negative_int(offset, "offset")
    if columns is not None:
        columns = affirm_type.list_of_str(columns, "columns")

    if self._is_scala:
        scala_data = self._scala.take(n, offset, self._tc.jutils.convert.to_scala_option_list_string(columns))
        schema = get_schema_for_columns(self.schema, columns) if columns else self.schema
        data = TakeCollectHelper.scala_rows_to_python(self._tc, scala_data, schema)
    else:
        require_type.non_negative_int(n, "n")
        if offset:
            data = _take_offset(self, n, offset, columns)
        elif columns:
            select_columns = TakeCollectHelper.get_select_columns_function(self.schema, columns)
            data = self._python.rdd.map(select_columns).take(n)
        else:
            data = self._python.rdd.take(n)
    return data


TakeRichResult = namedtuple("TakeResult", ['data', 'n', 'offset', 'schema'])


def take_rich(frame, n, offset=0, columns=None):
    """
    A take operation which also returns the schema, offset and count of the data.
    Not part of the "public" API, but used by other operations like inspect
    """
    if n is None:
        data = frame.collect(columns)
    else:
        data = frame.take(n, offset, columns)
    schema = frame.schema if not columns else sparktk.frame.schema.get_schema_for_columns(frame.schema, columns)
    return TakeRichResult(data=data, n=n, offset=offset, schema=schema)


def _take_offset(frame, n, offset, columns=None):
    """Helper to take from an offset in python (this could be relatively slow)"""
    select_columns = TakeCollectHelper.get_select_columns_function(frame.schema, columns) if columns else None

    count = 0
    data = []
    iterator = frame._python.rdd.toLocalIterator()
    try:
        row = iterator.next()
        while count < offset:
            count += 1
            row = iterator.next()
        while count < offset+n:
            data.append(select_columns(row) if select_columns else row)
            count += 1
            row = iterator.next()
    except StopIteration:
        pass
    return data


class TakeCollectHelper(object):
    """Helper class that has a few methods that both Take and Collect need"""

    @staticmethod
    def get_select_columns_function(schema, columns):
        """Returns a function which takes a row and returns a new row with swizzled and/or dropped columns"""
        if isinstance(columns, basestring):
            columns = [columns]
        elif not isinstance(columns, list):
            raise TypeError("columns must be be a string list of strings, but was %s" % type(columns))
        indices = sparktk.frame.schema.get_indices_for_selected_columns(schema, columns)

        def select_columns(row_array):
            return [row_array[index] for index in indices]
        return select_columns

    @staticmethod
    def scala_rows_to_python(tc, scala_data, schema):
        """converts list of lists of scala value to list of lists of python values, according to schema"""
        row_schema = schema

        def to_dtype(value, dtype):
            try:
                if type(dtype) == sparktk.dtypes.vector:
                    value = tc.jutils.convert.from_scala_vector(value)
                if type(dtype) == sparktk.dtypes._Matrix:
                    value = tc.jutils.convert.from_scala_matrix(value)
                return dtypes.cast(value, dtype)
            except:
                return None

        def scala_row_to_python(scala_row):
            num_cols = scala_row.length()
            return [to_dtype(scala_row.get(i), row_schema[i][1]) for i in xrange(num_cols)]

        python_data = map(scala_row_to_python, list(scala_data))

        return python_data
