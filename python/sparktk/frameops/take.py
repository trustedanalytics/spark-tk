from collections import namedtuple
import sparktk.schema

TakeResult = namedtuple("TakeResult", ['data', 'schema'])

def take(self, n, offset=0, columns=None):

    if offset != 0:
        raise NotImplementedError("take/inspect can't do offset yet")
    if self._is_scala:
        scala_data = self._scala.take(n)
        data = map(self._get_scala_row_to_python_converter(self.schema), scala_data)
    else:
        data = self._python.rdd.take(n)

    schema = self.schema
    if columns is not None:
        indices = sparktk.schema.get_indices_for_selected_columns(schema, columns)
        schema = sparktk.schema.get_schema_for_columns(schema, columns)
        data = extract_data_from_selected_columns(data, indices)

    return TakeResult(data=data, schema=schema)


def extract_data_from_selected_columns(data_in_page, indices):
    new_data = []
    for row in data_in_page:
        new_data.append([row[index] for index in indices])

    return new_data

