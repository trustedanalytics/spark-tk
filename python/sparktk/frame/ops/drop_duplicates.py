def drop_duplicates(self, unique_columns=None):
    """
    Modify the current frame, removing duplicate rows.

    :param unique_columns: Column name(s) to identify duplicates. Default is the entire row is compared.

    Remove data rows which are the same as other rows.
    The entire row can be checked for duplication, or the search for duplicates can be limited to one or more columns.
    This modifies the current frame.

    Examples
    --------

    Given a frame with data:

    .. code::

        <hide>
        >>> frame = tc.to_frame([[200, 4, 25],
        ...                     [200, 5, 25],
        ...                     [200, 4, 25],
        ...                     [200, 5, 35],
        ...                     [200, 6, 25],
        ...                     [200, 8, 35],
        ...                     [200, 4, 45],
        ...                     [200, 4, 25],
        ...                     [200, 5, 25],
        ...                     [201, 4, 25]],
        ...                     [("a", int), ("b", int), ("c", int)])
        <progress>

        </hide>

        >>> frame.inspect()
        [#]  a    b  c
        ===============
        [0]  200  4  25
        [1]  200  5  25
        [2]  200  4  25
        [3]  200  5  35
        [4]  200  6  25
        [5]  200  8  35
        [6]  200  4  45
        [7]  200  4  25
        [8]  200  5  25
        [9]  201  4  25

    Remove any rows that are identical to a previous row.
    The result is a frame of unique rows.
    Note that row order may change.

    .. code::

        >>> frame.drop_duplicates()
        <progress>

        >>> frame.inspect()
        [#]  a    b  c
        ===============
        [0]  200  8  35
        [1]  200  6  25
        [2]  200  5  35
        [3]  200  4  45
        [4]  200  4  25
        [5]  200  5  25
        [6]  201  4  25

    Now remove any rows that have the same data in columns *a* and
    *c* as a previously checked row:

    .. code::

        >>> frame.drop_duplicates([ "a", "c"])
        <progress>

    The result is a frame with unique values for the combination of columns *a*
    and *c*.

    .. code::

        >>> frame.inspect()
        [#]  a    b  c
        ===============
        [0]  201  4  25
        [1]  200  4  45
        [2]  200  6  25
        [3]  200  8  35

    """
    if unique_columns is not None and not isinstance(unique_columns, list):
        unique_columns = [unique_columns]
    if isinstance(unique_columns, list):
        unique_columns = self._tc.jutils.convert.to_scala_vector_string(unique_columns)
    self._scala.dropDuplicates(self._tc.jutils.convert.to_scala_option(unique_columns))