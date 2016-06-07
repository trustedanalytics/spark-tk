def tally_percent(self, sample_col, count_val):
    """
    Compute a cumulative percent count.

    :param sample_col: The name of the column from which to compute the cumulative sum.
    :param count_val: The column value to be used for the counts.

    A cumulative percent count is computed by sequentially stepping through
    the rows, observing the column values and keeping track of the percentage of the
    total number of times the specified *count_value* has been seen up to
    the current value.

    Examples
    --------
    Consider Frame *my_frame*, which accesses a frame that contains a single
    column named *obs*:

        <hide>
        >>> my_frame = tc.frame.create([[0],[1],[2],[0],[1],[2]], [("obs", int)])
        -etc-

        </hide>
        >>> my_frame.inspect()
        [#]  obs
        ========
        [0]    0
        [1]    1
        [2]    2
        [3]    0
        [4]    1
        [5]    2

    The cumulative percent count for column *obs* is obtained by:

        >>> my_frame.tally_percent("obs", "1")
        <progress>

    The Frame *my_frame* accesses the original frame that now contains two
    columns, *obs* that contains the original column values, and
    *obsCumulativePercentCount* that contains the cumulative percent count:

        >>> my_frame.inspect()
        [#]  obs  obs_tally_percent
        ===========================
        [0]    0                0.0
        [1]    1                0.5
        [2]    2                0.5
        [3]    0                0.5
        [4]    1                1.0
        [5]    2                1.0

    """
    self._scala.tallyPercent(sample_col, count_val)