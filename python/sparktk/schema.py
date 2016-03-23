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

