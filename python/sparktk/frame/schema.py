from sparktk.dtypes import dtypes

def jvm_scala_schema(sc):
    return sc._jvm.org.trustedanalytics.sparktk.frame.SchemaHelper

def get_schema_for_columns(schema, selected_columns):
    indices = get_indices_for_selected_columns(schema, selected_columns)
    return [schema[i] for i in indices]

def get_indices_for_selected_columns(schema, selected_columns):
    indices = []
    schema_columns = [col[0] for col in schema]
    if not selected_columns:
        raise ValueError("Column list must not be empty. Please choose from : (%s)" % ",".join(schema_columns))
    for column in selected_columns:
        try:
            indices.append(schema_columns.index(column))
        except:
            raise ValueError("Invalid column name %s provided"
                             ", please choose from: (%s)" % (column, ",".join(schema_columns)))

    return indices

def schema_to_scala(sc, python_schema):
    list_of_list_of_str_schema = map(lambda t: [t[0], dtypes.to_string(t[1])], python_schema)  # convert dtypes to strings
    return jvm_scala_schema(sc).pythonToScala(list_of_list_of_str_schema)

def schema_to_python(sc, scala_schema):
    list_of_list_of_str_schema = jvm_scala_schema(sc).scalaToPython(scala_schema)
    return [(name, dtypes.get_from_string(dtype)) for name, dtype in list_of_list_of_str_schema]

def is_mergeable(tc, *python_schema):

    scala_schema_list = []
    for schema in python_schema:
        if not isinstance(schema, list):
            schema = [schema]
        scala_schema_list.append(schema_to_scala(tc.sc, schema))

    return jvm_scala_schema(tc.sc).isMergeable(tc.jutils.convert.to_scala_list(scala_schema_list))
