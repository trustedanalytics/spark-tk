
def bin_column(self, column_name, cutoffs, include_lowest=True, strict_binning=False, bin_column_name=None):
    """
    :param column_name: Name of the column to bin
    :param cutoffs: Array of values containing bin cutoff points. Array can be list or tuple. Array values must be
     progressively increasing. All bin boundaries must be included, so, with N bins, you need N+1 values.
    :param include_lowest: Specify how the boundary conditions are handled. ``True`` indicates that the lower bound
     of the bin is inclusive. ``False`` indicates that the upper bound is inclusive. Default is ``True``.
    :param strict_binning: Specify how values outside of the cutoffs array should be binned. If set to ``True``, each
     value less than cutoffs[0] or greater than cutoffs[-1] will be assigned a bin value of -1. If set to ``False``,
     values less than cutoffs[0] will be included in the first bin while values greater than cutoffs[-1] will be
     included in the final bin.
    :param bin_column_name: The name for the new binned column.  Default is ``<column_name>_binned``


    Examples
    --------
    For these examples, we will use a frame with column *a* accessed by a Frame
    object *my_frame*:

    <hide>

    >>> frame = tc.to_frame([[1],[1],[2],[3],[5],[8],[13],[21],[34],[55],[89]], [('a', int)])
    -etc-

    </hide>
    >>> frame.inspect(n=11)
    [##]  a
    ========
    [0]    1
    [1]    1
    [2]    2
    [3]    3
    [4]    5
    [5]    8
    [6]   13
    [7]   21
    [8]   34
    [9]   55
    [10]  89

    Modify the frame with a column showing what bin the data is in.
    The data values should use strict_binning:

    >>> frame.bin_column('a', [5, 12, 25, 60], include_lowest=True,
    ... strict_binning=True, bin_column_name='binned')
    <progress>

    >>> frame.inspect(n=11)
    [##]  a   binned
    ================
    [0]    1      -1
    [1]    1      -1
    [2]    2      -1
    [3]    3      -1
    [4]    5       0
    [5]    8       0
    [6]   13       1
    [7]   21       1
    [8]   34       2
    [9]   55       2
    [10]  89      -1

    <hide>
    >>> frame.drop_columns('binned')
    -etc-

    </hide>
    Modify the frame with a column showing what bin the data is in.
    The data value should not use strict_binning:


    >>> frame.bin_column('a', [5, 12, 25, 60], include_lowest=True,
    ... strict_binning=False, bin_column_name='binned')
    <progress>

    >>> frame.inspect(n=11)
    [##]  a   binned
    ================
    [0]    1       0
    [1]    1       0
    [2]    2       0
    [3]    3       0
    [4]    5       0
    [5]    8       0
    [6]   13       1
    [7]   21       1
    [8]   34       2
    [9]   55       2
    [10]  89       2

    <hide>
    >>> frame.drop_columns('binned')
    -etc-

    </hide>
    Modify the frame with a column showing what bin the data is in.
    The bins should be lower inclusive:

    >>> frame.bin_column('a', [1,5,34,55,89], include_lowest=True,
    ... strict_binning=False, bin_column_name='binned')
    <progress>

    >>> frame.inspect( n=11 )
    [##]  a   binned
    ================
    [0]    1       0
    [1]    1       0
    [2]    2       0
    [3]    3       0
    [4]    5       1
    [5]    8       1
    [6]   13       1
    [7]   21       1
    [8]   34       2
    [9]   55       3
    [10]  89       3

    <hide>
    >>> frame.drop_columns('binned')
    -etc-

    </hide>
    Modify the frame with a column showing what bin the data is in.
    The bins should be upper inclusive:

    >>> frame.bin_column('a', [1,5,34,55,89], include_lowest=False,
    ... strict_binning=True, bin_column_name='binned')
    <progress>

    >>> frame.inspect( n=11 )
    [##]  a   binned
    ================
    [0]    1       0
    [1]    1       0
    [2]    2       0
    [3]    3       0
    [4]    5       0
    [5]    8       1
    [6]   13       1
    [7]   21       1
    [8]   34       1
    [9]   55       2
    [10]  89       3

    """

    self._scala.binColumn(column_name,
                          self._tc.jutils.convert.to_scala_list_double([float(c) for c in cutoffs]),
                          include_lowest,
                          strict_binning,
                          self._tc.jutils.convert.to_scala_option(bin_column_name))
