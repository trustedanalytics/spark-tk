
def unflatten_columns (self, columns, delimiter=None):
    """
    Compacts data from multiple rows based on cell data.

    :param columns: Name of the column(s) to be used as keys for unflattening.
    :param delimiter: Separator for the data in the result columns.  Default is comma (,).

    Groups together cells in all columns (less the composite key) using "," as string delimiter.
    The original rows are deleted.
    The grouping takes place based on a composite key created from cell values.
    The column datatypes are changed to string.

    Examples
    --------
    <hide>

    >>> frame = tc.to_frame([["user1", "1/1/2015", 1, 70],["user1", "1/1/2015", 2, 60],["user2", "1/1/2015", 1, 65]],
    ...                     [('a', str), ('b', str),('c', int),('d', int)])
    <progress>

    </hide>
    Given a data file::

        user1 1/1/2015 1 70
        user1 1/1/2015 2 60
        user2 1/1/2015 1 65

    The commands to bring the data into a frame, where it can be worked on:

    .. code::

        >>> frame.inspect()
        [#]  a      b         c  d
        ===========================
        [0]  user1  1/1/2015  1  70
        [1]  user1  1/1/2015  2  60
        [2]  user2  1/1/2015  1  65


    Unflatten the data using columns a & b:

    .. code::

        >>> frame.unflatten_columns(['a','b'])
        <progress>

    Check again:

    .. code::

        >>> frame.inspect()
        [#]  a      b         c    d
        ================================
        [0]  user1  1/1/2015  1,2  70,60
        [1]  user2  1/1/2015  1    65


    Alternatively, unflatten_columns() also accepts a single column like:

    <hide>

    # Re-create frame with original data to start over with single column example
    >>> frame = tc.to_frame([["user1", "1/1/2015", 1, 70],["user1", "1/1/2015", 2, 60],["user2", "1/1/2015", 1, 65]],
    ...                     [('a', str), ('b', str),('c', int) ,('d', int)])
    <progress>

    </hide>

    >>> frame.unflatten_columns('a')
    <progress>

    >>> frame.inspect()
    [#]  a      b                  c    d
    =========================================
    [0]  user1  1/1/2015,1/1/2015  1,2  70,60
    [1]  user2  1/1/2015           1    65

    """
    if not isinstance(columns, list):
        columns = [columns]
    return self._scala.unflattenColumns(self._tc.jutils.convert.to_scala_list_string(columns),
                                        self._tc.jutils.convert.to_scala_option(delimiter))