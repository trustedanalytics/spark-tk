import sparktk.frame.schema

def sort(self, columns, ascending=True):
    """
    Sort by one or more columns.

    :param columns: Either a column name, list of column names, or list of tuples wher eeach tuple is a name and an
                    ascending bool value.
    :param ascending: True for ascending, False for descending

    Sort a frame by column values either ascending or descending.


    Examples
    --------

        <hide>
        >>> frame = tc.to_frame([[3, 'foxtrot'], [1, 'charlie'], [3, 'bravo'], [2, 'echo'], [4, 'delta'], [3, 'alpha']], [('col1', int), ('col2', str)])
        -etc-

        </hide>

    Consider the frame
        >>> frame.inspect()
        [#]  col1  col2
        ==================
        [0]     3  foxtrot
        [1]     1  charlie
        [2]     3  bravo
        [3]     2  echo
        [4]     4  delta
        [5]     3  alpha

    Sort a single column:

    .. code::

        >>> frame.sort('col1')
        <progress>
        >>> frame.inspect()
        [#]  col1  col2
        ==================
        [0]     1  charlie
        [1]     2  echo
        [2]     3  foxtrot
        [3]     3  bravo
        [4]     3  alpha
        [5]     4  delta

    Sort a single column descending:

    .. code::

        >>> frame.sort('col2', False)
        <progress>
        >>> frame.inspect()
        [#]  col1  col2
        ==================
        [0]     3  foxtrot
        [1]     2  echo
        [2]     4  delta
        [3]     1  charlie
        [4]     3  bravo
        [5]     3  alpha

    Sort multiple columns:

    .. code::

        >>> frame.sort(['col1', 'col2'])
        <progress>
        >>> frame.inspect()
        [#]  col1  col2
        ==================
        [0]     1  charlie
        [1]     2  echo
        [2]     3  alpha
        [3]     3  bravo
        [4]     3  foxtrot
        [5]     4  delta


    Sort multiple columns descending:

    .. code::

        >>> frame.sort(['col1', 'col2'], False)
        <progress>
        >>> frame.inspect()
        [#]  col1  col2
        ==================
        [0]     4  delta
        [1]     3  foxtrot
        [2]     3  bravo
        [3]     3  alpha
        [4]     2  echo
        [5]     1  charlie

    Sort multiple columns: 'col1' decending and 'col2' ascending:

    .. code::

        >>> frame.sort([ ('col1', False), ('col2', True) ])
        <progress>
        >>> frame.inspect()
        [#]  col1  col2
        ==================
        [0]     4  delta
        [1]     3  alpha
        [2]     3  bravo
        [3]     3  foxtrot
        [4]     2  echo
        [5]     1  charlie

    """
    if columns is None:
        raise ValueError("The columns parameter should not be None.")
    elif not isinstance(columns, list):
        columns = [columns]
    if not columns:
        raise ValueError("The columns parameter should not be empty.")
    if self._is_scala:
        scala_sort(self, columns, ascending)
    else:
        column_names = columns              # list of column names
        columns_ascending = ascending       # boolean summarizing if we are sorting ascending or descending
        sort_using_scala = False
        if isinstance(columns[0], tuple):
            if isinstance(columns[0][1], bool):
                columns_ascending = columns[0][1]
            else:
                raise ValueError("If the column parameter is a list of tuples, each tuple should have a string"
                                 "(column name) and bool (ascending), but found %s for the second tuple item." % type(columns[0][1]))
            column_names = []
            for column_tuple in columns:
                if isinstance(column_tuple[0], str):
                    column_names.append(column_tuple[0])
                else:
                    raise ValueError("If the column parameter is a list of tuples, each tuple should have a string"
                                     "(column name) and bool (ascending), but found %s for the first tuple item." % type(column_tuple[0]))
                if isinstance(column_tuple[1], bool):
                    if column_tuple[1] != columns_ascending:
                        # If the user is mix ascending and descending for different columns, use scala to sort.
                        sort_using_scala = True
                        break
                else:
                    raise ValueError("If the column parameter is a list of tuples, each tuple should have a string"
                                     "(column name) and bool (ascending), but found %s for the second tuple item." % type(columns[0][1]))
        if sort_using_scala:
            scala_sort(self, columns, ascending)
        else:
            indices = sparktk.frame.schema.get_indices_for_selected_columns(self.schema, column_names)
            self._python.rdd = self.rdd.sortBy(lambda x: tuple([x[index] for index in indices]), ascending=columns_ascending)

def scala_sort(self, columns, ascending):
    if isinstance(columns[0], basestring):
        columns_and_ascending = map(lambda x: (x, ascending),columns)
    else:
        columns_and_ascending = columns
    self._scala.sort(self._tc.jutils.convert.to_scala_list_string_bool_tuple(columns_and_ascending))