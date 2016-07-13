import sparktk.frame.schema

def rename_columns(self, names):
    """
    Rename columns

    Parameters
    ----------

    :param names: (dict) Dictionary of old names to new names.

    Examples
    --------
    Start with a frame with columns *Black* and *White*.

        <hide>

        >>> s = [('Black', unicode), ('White', unicode)]
        >>> rows = [["glass", "clear"],["paper","unclear"]]
        >>> my_frame = tc.frame.create(rows, s)
        -etc-

        </hide>

        >>> print my_frame.schema
        [('Black', <type 'unicode'>), ('White', <type 'unicode'>)]

    Rename the columns to *Mercury* and *Venus*:

        >>> my_frame.rename_columns({"Black": "Mercury", "White": "Venus"})

        >>> print my_frame.schema
        [('Mercury', <type 'unicode'>), ('Venus', <type 'unicode'>)]

    """
    if not isinstance(names, dict):
        raise ValueError("Unsupported 'names' parameter type.  Expected dictionary, but found %s." % type(names))
    if self.schema is None:
        raise RuntimeError("Unable rename column(s), because the frame's schema has not been defined.")

    new_conlumns_names = [names[v] for v in names]
    if len(new_conlumns_names) == len(set(new_conlumns_names)):
        if self._is_python:
            new_schema = self._python.schema
            index_list = sparktk.frame.schema.get_indices_for_selected_columns(self.schema, names.keys())
            for index in index_list:
                old_name = new_schema[index][0]
                data_type = new_schema[index][1]
                new_name = names[old_name]
                if new_name in self.column_names and len(set(set(names)).intersection(set(names.values()))) == 0:
                    raise ValueError("Unable rename column(s), because the column's name %s already exists." %new_name)
                else:
                    new_schema[index] = (new_name, data_type)
            self._python.schema = new_schema
        else:
            self._scala.renameColumns(self._tc.jutils.convert.to_scala_map(names))
    else:
        raise ValueError("Unable rename columns, because the columns names %s are not unique." %new_conlumns_names)