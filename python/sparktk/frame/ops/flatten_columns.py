
def flatten_columns (self, columns, delimiters=None):
    """
    Spread data to multiple rows based on cell data.

    :param columns: The columns to be flattened.
    :param delimiters: The list of delimiter strings for each column.  Default is comma (,).

    Splits cells in the specified columns into multiple rows according to a string delimiter.
    New rows are a full copy of the original row, but the specified columns only contain one value.
    The original row is deleted.

    Examples
    --------
    <hide>
    >>> frame = tc.to_frame([[1,"solo,mono,single","green,yellow,red"],[2,"duo,double","orange,black"]],
    ...                     [('a',int),('b', str),('c', str)])
    <progress>

    </hide>

    Given a data file::

        1-solo,mono,single-green,yellow,red
        2-duo,double-orange,black

    The commands to bring the data into a frame, where it can be worked on:

    .. code::

        >>> frame.inspect()
        [#]  a  b                 c
        ==========================================
        [0]  1  solo,mono,single  green,yellow,red
        [1]  2  duo,double        orange,black

    Now, spread out those sub-strings in column *b* and *c*:

    .. code::

        >>> frame.flatten_columns(['b','c'], ',')
        <progress>

    Note that the delimiters parameter is optional, and if no delimiter is specified, the default
    is a comma (,).  So, in the above example, the delimiter parameter could be omitted.  Also, if
    the delimiters are different for each column being flattened, a list of delimiters can be
    provided.  If a single delimiter is provided, it's assumed that we are using the same delimiter
    for all columns that are being flattened.  If more than one delimiter is provided, the number of
    delimiters must match the number of string columns being flattened.

    Check again:

    .. code::

        >>> frame.inspect()
        [#]  a  b       c
        ======================
        [0]  1  solo    green
        [1]  1  mono    yellow
        [2]  1  single  red
        [3]  2  duo     orange
        [4]  2  double  black

    <hide>
    >>> frame = tc.to_frame([[1,"solo,mono,single","green,yellow,red"],[2,"duo,double","orange,black"]],
    ...                     [('a',int),('b', str),('c', str)])
    <progress>

    </hide>

    Alternatively, flatten_columns also accepts a single column name (instead of a list) if just one
    column is being flattened.  For example, we could have called flatten_column on just column *b*:


    .. code::

        >>> frame.flatten_columns('b', ',')
        <progress>

    Check again:

    .. code ::

        >>> frame.inspect()
        [#]  a  b       c
        ================================
        [0]  1  solo    green,yellow,red
        [1]  1  mono    green,yellow,red
        [2]  1  single  green,yellow,red
        [3]  2  duo     orange,black
        [4]  2  double  orange,black

    """
    if not isinstance(columns, list):
        columns = [columns]
    if not isinstance(delimiters, list):
        delimiters = [delimiters]
    return self._scala.flattenColumns(self._tc.jutils.convert.to_scala_list_string(columns),
                                      self._tc.jutils.convert.to_scala_option_list_string(delimiters))