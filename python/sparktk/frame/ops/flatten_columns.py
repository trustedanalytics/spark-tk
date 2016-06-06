
def flatten_columns (self, *columns):
    """
    Spread data to multiple rows based on cell data.

    :param columns: The the name of the column to be flattened, or a tuple with the column name and
                    delimiter string.  The default delimiter is a comma (,).

    Splits cells in the specified columns into multiple rows according to a string delimiter.
    New rows are a full copy of the original row, but the specified columns only contain one value.
    The original row is deleted.

    Examples
    --------
    <hide>
    >>> frame = tc.frame.create([[1, "solo,mono,single", "green|yellow|red"],
    ...                          [2, "duo,double", "orange|black"]],
    ...                         [('a', int), ('b', str), ('c', str)])
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
        [0]  1  solo,mono,single  green|yellow|red
        [1]  2  duo,double        orange|black

    Now, spread out those sub-strings in column *b* and *c* by specifying the column names and delmiters:

    .. code::

        >>> frame.flatten_columns(('b', ','), ('c', '|'))
        <progress>

    Note that the delimiters parameter is optional, and if no delimiter is specified, the default
    is a comma (,).  So, in the above example, the delimiter parameter for *b* could be omitted.

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
    >>> frame = tc.frame.create([[1, "solo,mono,single", "green|yellow|red"],
    ...                          [2, "duo,double", "orange|black"]],
    ...                         [('a', int), ('b', str), ('c', str)])
    <progress>

    </hide>

    Alternatively, we can flatten a single column *b* using the default comma delimiter:

    .. code::

        >>> frame.flatten_columns('b')
        <progress>

    Check again:

    .. code ::

        >>> frame.inspect()
        [#]  a  b       c
        ================================
        [0]  1  solo    green|yellow|red
        [1]  1  mono    green|yellow|red
        [2]  1  single  green|yellow|red
        [3]  2  duo     orange|black
        [4]  2  double  orange|black

    """
    columns = [c if isinstance(c, tuple) else (c, None) for c in columns]
    return self._scala.flattenColumns(self._tc.jutils.convert.to_scala_list_string_option_tuple(columns))