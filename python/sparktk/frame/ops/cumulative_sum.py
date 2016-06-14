def cumulative_sum(self, sample_col):
    """
    Add column to frame with cumulative sum.

    Parameters
    ----------

    :param sample_col: (str) The name of the column from which to compute the cumulative sum.

    A cumulative sum is computed by sequentially stepping through the rows,
    observing the column values and keeping track of the cumulative sum for each value.

    Notes
    -----
    This method applies only to columns containing numerical data.

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

    The cumulative sum for column *obs* is obtained by:

        >>> my_frame.cumulative_sum('obs')
        <progress>

    The Frame *my_frame* accesses the original frame that now contains two
    columns, *obs* that contains the original column values, and
    *obsCumulativeSum* that contains the cumulative percent count:

        >>> my_frame.inspect()
        [#]  obs  obs_cumulative_sum
        ============================
        [0]    0                 0.0
        [1]    1                 1.0
        [2]    2                 3.0
        [3]    0                 3.0
        [4]    1                 4.0
        [5]    2                 6.0

    """
    self._scala.cumulativeSum(sample_col)