from collections import namedtuple

import sparktk.frame.schema
from sparktk.dtypes import dtypes

TakeResult = namedtuple("TakeResult", ['data', 'schema'])

def take(self, n, offset=0, columns=None):

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
