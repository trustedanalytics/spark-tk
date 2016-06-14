
def quantile_bin_column(self, column_name, num_bins=None, bin_column_name=None):
    """
    Classify column into groups with the same frequency.

    Group rows of data based on the value in a single column and add a label
    to identify grouping.

    Equal depth binning attempts to label rows such that each bin contains the
    same number of elements.
    For :math:`n` bins of a column :math:`C` of length :math:`m`, the bin
    number is determined by:

    .. math::

        \lceil n * \frac { f(C) }{ m } \rceil

    where :math:`f` is a tie-adjusted ranking function over values of
    :math:`C`.
    If there are multiples of the same value in :math:`C`, then their
    tie-adjusted rank is the average of their ordered rank values.

    Notes
    -----

    1.  The num_bins parameter is considered to be the maximum permissible number
        of bins because the data may dictate fewer bins.
        For example, if the column to be binned has a quantity of :math"`X`
        elements with only 2 distinct values and the *num_bins* parameter is
        greater than 2, then the actual number of bins will only be 2.
        This is due to a restriction that elements with an identical value must
        belong to the same bin.

    Parameters
    ----------

    :param column_name: (str) The column whose values are to be binned.
    :param num_bins: (Optional[int]) The maximum number of quantiles.
                     Default is the Square-root choice
                     :math:`\lfloor \sqrt{m} \rfloor`, where :math:`m` is the number of rows.
    :param bin_column_name: (Optional[str]) The name for the new column holding the grouping labels.
                            Default is <column_name>_binned
    :return: (List[float]) A list containing the edges of each bin

    Examples
    --------
    Given a frame with column *a* accessed by a Frame object *my_frame*:

    <hide>

        >>> my_frame = tc.frame.create([[1],[1],[2],[3],[5],[8],[13],[21],[34],[55],[89]],
        ... [('a', int)])
        -etc-

    </hide>

        >>> my_frame.inspect( n=11 )
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


    Modify the frame, adding a column showing what bin the data is in.
    The data should be grouped into a maximum of five bins.
    Note that each bin will have the same quantity of members (as much as
    possible):

        >>> cutoffs = my_frame.quantile_bin_column('a', 5, 'aEDBinned')
        <progress>

        >>> my_frame.inspect( n=11 )
        [##]  a   aEDBinned
        ===================
        [0]    1          0
        [1]    1          0
        [2]    2          1
        [3]    3          1
        [4]    5          2
        [5]    8          2
        [6]   13          3
        [7]   21          3
        [8]   34          4
        [9]   55          4
        [10]  89          4

        >>> print cutoffs
        [1.0, 2.0, 5.0, 13.0, 34.0, 89.0]

    """
    return self._tc.jutils.convert.from_scala_seq(self._scala.quantileBinColumn(column_name,
                                                  self._tc.jutils.convert.to_scala_option(num_bins),
                                                  self._tc.jutils.convert.to_scala_option(bin_column_name)))