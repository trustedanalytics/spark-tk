
def bin_column(self, column_name, bins=None, include_lowest=True, strict_binning=False, bin_column_name=None):
    """
    Summarize rows of data based on the value in a single column by sorting them
    into bins, or groups, based on a list of bin cutoff points or a specified number of
    equal-width bins.

    **Notes**

    #)  Bins IDs are 0-index, in other words, the lowest bin number is 0.
    #)  The first and last cutoffs are always included in the bins.
        When *include_lowest* is ``True``, the last bin includes both cutoffs.
        When *include_lowest* is ``False``, the first bin (bin 0) includes both
        cutoffs.

    :param column_name: Name of the column to bin
    :param bins: Either a single value representing the number of equal-width bins to create, or an array of values
     containing bin cutoff points. Array can be list or tuple. If an array is provided, values must be progressively
     increasing. All bin boundaries must be included, so, with N bins, you need N+1 values.
     Default is equal-width bins where the maximum number of bins is the Square-root choice
     :math:`\lfloor \sqrt{m} \rfloor`, where :math:`m` is the number of rows.
    :param include_lowest: Specify how the boundary conditions are handled. ``True`` indicates that the lower bound
     of the bin is inclusive. ``False`` indicates that the upper bound is inclusive. Default is ``True``.
    :param strict_binning: Specify how values outside of the cutoffs array should be binned. If set to ``True``, each
     value less than cutoffs[0] or greater than cutoffs[-1] will be assigned a bin value of -1. If set to ``False``,
     values less than cutoffs[0] will be included in the first bin while values greater than cutoffs[-1] will be
     included in the final bin.
    :param bin_column_name: The name for the new binned column.  Default is ``<column_name>_binned``
    :return: a list containing the edges of each bin

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

    Modify the frame with a column showing what bin the data is in, by
    specifying cutoffs for the bin edges.
    The data values should use strict_binning:

    >>> frame.bin_column('a', [5, 12, 25, 60], include_lowest=True,
    ... strict_binning=True, bin_column_name='binned_using_cutoffs')
    <progress>

    >>> frame.inspect(n=11)
    [##]  a   binned_using_cutoffs
    ==============================
    [0]    1                    -1
    [1]    1                    -1
    [2]    2                    -1
    [3]    3                    -1
    [4]    5                     0
    [5]    8                     0
    [6]   13                     1
    [7]   21                     1
    [8]   34                     2
    [9]   55                     2
    [10]  89                    -1

    <hide>
    >>> frame.drop_columns('binned_using_cutoffs')
    -etc-

    </hide>

    Modify the frame with a column showing what bin the data is in.
    The data value should not use strict_binning:

    >>> frame.bin_column('a', [5, 12, 25, 60], include_lowest=True,
    ... strict_binning=False, bin_column_name='binned_using_cutoffs')
    <progress>

    >>> frame.inspect(n=11)
    [##]  a   binned_using_cutoffs
    ==============================
    [0]    1                     0
    [1]    1                     0
    [2]    2                     0
    [3]    3                     0
    [4]    5                     0
    [5]    8                     0
    [6]   13                     1
    [7]   21                     1
    [8]   34                     2
    [9]   55                     2
    [10]  89                     2

    <hide>
    >>> frame.drop_columns('binned_using_cutoffs')
    -etc-

    </hide>
    Modify the frame with a column showing what bin the data is in.
    The bins should be lower inclusive:

    >>> frame.bin_column('a', [1,5,34,55,89], include_lowest=True,
    ... strict_binning=False, bin_column_name='binned_using_cutoffs')
    <progress>

    >>> frame.inspect( n=11 )
    [##]  a   binned_using_cutoffs
    ==============================
    [0]    1                     0
    [1]    1                     0
    [2]    2                     0
    [3]    3                     0
    [4]    5                     1
    [5]    8                     1
    [6]   13                     1
    [7]   21                     1
    [8]   34                     2
    [9]   55                     3
    [10]  89                     3

    <hide>
    >>> frame.drop_columns('binned_using_cutoffs')
    -etc-

    </hide>
    Modify the frame with a column showing what bin the data is in.
    The bins should be upper inclusive:

    >>> frame.bin_column('a', [1,5,34,55,89], include_lowest=False,
    ... strict_binning=True, bin_column_name='binned_using_cutoffs')
    <progress>

    >>> frame.inspect( n=11 )
    [##]  a   binned_using_cutoffs
    ==============================
    [0]    1                     0
    [1]    1                     0
    [2]    2                     0
    [3]    3                     0
    [4]    5                     0
    [5]    8                     1
    [6]   13                     1
    [7]   21                     1
    [8]   34                     1
    [9]   55                     2
    [10]  89                     3

    <hide>
    >>> frame.drop_columns('binned_using_cutoffs')
    -etc-

    </hide>
    Modify the frame with a column of 3 equal-width bins.  This also
    returns the cutoffs that were used for creating the bins.

    >>> cutoffs = frame.bin_column('a', 3, bin_column_name='equal_width_bins')

    >>> print cutoffs
    [1.0, 30.333333333333332, 59.666666666666664, 89.0]

    >>> frame.inspect(n=frame.count())
    [##]  a   equal_width_bins
    ==========================
    [0]    1                 0
    [1]    1                 0
    [2]    2                 0
    [3]    3                 0
    [4]    5                 0
    [5]    8                 0
    [6]   13                 0
    [7]   21                 0
    [8]   34                 1
    [9]   55                 1
    [10]  89                 2

    """
    if not isinstance(bins, list):
        bins = [bins]
    return list(self._scala.binColumn(column_name,
                                self._tc.jutils.convert.to_scala_option_list_double(bins),
                                include_lowest,
                                strict_binning,
                                self._tc.jutils.convert.to_scala_option(bin_column_name)))