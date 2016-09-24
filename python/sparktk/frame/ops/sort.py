import sparktk.frame.schema

def sort(self, columns, ascending=True):
    """
    Sort by one or more columns.

    Parameters
    ----------

    :param columns: (str or List[str]) Either a column name, list of column names, or list of tuples where each tuple is a name and an
                    ascending bool value.
    :param ascending: (Optional[bool]) True for ascending (default), or False for descending.

    Sort a frame by column values either ascending or descending.

    Examples
    --------

        <hide>
        >>> frame = tc.frame.create([[3, 'foxtrot'], [1, 'charlie'], [3, 'bravo'], [2, 'echo'], [4, 'delta'], [3, 'alpha']], [('col1', int), ('col2', str)])
        -etc-

        </hide>

    Consider the frame:

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

        if isinstance(columns[0], tuple):
            are_all_proper_tuples = all(isinstance(c, tuple) and isinstance(c[0], basestring) and isinstance(c[1], bool) for c in columns)

            if not are_all_proper_tuples:
                raise ValueError("If the columns paramter is a list of tuples, each tuple must have a string (column name)"
                                 "and a bool (True for ascending).")

            column_names = [c[0] for c in columns]  # Grab just the column names from the list of tuples

            # Check ascending booleans in the tuples to see if they're all the same
            are_all_same_ascending = all(c[1] == columns[0][1] for c in columns)

            if are_all_same_ascending:
                columns_ascending = columns[0][1]
        else:
            are_all_same_ascending = True

        if are_all_same_ascending:
            indices = sparktk.frame.schema.get_indices_for_selected_columns(self.schema, column_names)
            self._python.rdd = self.rdd.sortBy(lambda x: tuple([x[index] for index in indices]), ascending=columns_ascending)

        else:
            # If there are different ascending values between columns, then use scala sort
            scala_sort(self, columns, ascending)

def scala_sort(self, columns, ascending):
    if isinstance(columns[0], basestring):
        columns_and_ascending = [(c, ascending) for c in columns]
    else:
        columns_and_ascending = columns
    self._scala.sort(self._tc.jutils.convert.to_scala_list_string_bool_tuple(columns_and_ascending))