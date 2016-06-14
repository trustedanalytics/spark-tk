from collections import namedtuple

import sparktk.frame.schema
from sparktk.dtypes import dtypes

TakeResult = namedtuple("TakeResult", ['data', 'schema'])

def take(self, n, offset=0, columns=None):
    """
    Get data subset.

    Take a subset of the currently active Frame.

    Parameters
    ----------

    :param n: (int) The number of rows to copy from the frame.
    :param offset: (Optional[int]) The number of rows to skip before starting to copy.
    :param columns: (Optional[str or List[str]) If not None, only the given columns' data will be provided.
                    By default, all columns are included.
    :return: (TakeResult) Data and schema for the requested dataset

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

        >>> result = frame.take(2)

        >>> result.schema
        [('name', <type 'str'>), ('age', <type 'int'>), ('tenure', <type 'int'>), ('phone', <type 'str'>)]

        >>> result.data
        [['Fred', 39, 16, '555-1234'], ['Susan', 33, 3, '555-0202']]

    Limit the columns in our result to just the name and age column:

        >>> result = frame.take(2, columns=['name', 'age'])

    Note that the schema and data in the result only have name and age information:

        >>> result.schema
        [('name', <type 'str'>), ('age', <type 'int'>)]

        >>> result.data
        [['Fred', 39], ['Susan', 33]]

    """
    if offset != 0:
        raise NotImplementedError("take/inspect can't do offset yet")
    if self._is_scala:
        scala_data = self._scala.take(n)
        data = map(_get_scala_row_to_python_converter(self._tc, self.schema), scala_data)
    else:
        data = self._python.rdd.take(n)

    schema = self.schema
    if columns is not None:
        indices = sparktk.frame.schema.get_indices_for_selected_columns(schema, columns)
        schema = sparktk.frame.schema.get_schema_for_columns(schema, columns)
        data = extract_data_from_selected_columns(data, indices)

    return TakeResult(data=data, schema=schema)


def extract_data_from_selected_columns(data_in_page, indices):
    new_data = []
    for row in data_in_page:
        new_data.append([row[index] for index in indices])

    return new_data


def _get_scala_row_to_python_converter(tc, schema):
    """gets a converter for going from scala value to python value, according to dtype"""
    row_schema = schema

    def to_dtype(value, dtype):
        try:
            if type(dtype) == sparktk.dtypes.vector:
                value = tc.jutils.convert.from_scala_vector(value)
            return dtypes.cast(value, dtype)
        except:
            return None

    def scala_row_to_python(scala_row):
        num_cols = scala_row.length()
        return [to_dtype(scala_row.get(i), row_schema[i][1]) for i in xrange(num_cols)]

    return scala_row_to_python
