
def ecdf(self, column):
    """
    Builds new frame with columns for data and distribution.

    :param column: The name of the input column containing sample.
    :return: A new Frame containing each distinct value in the sample and its corresponding ECDF value.

    Generates the :term:`empirical cumulative distribution` for the input column.

    Examples
    --------

    Consider the following sample data set in *frame* 'frame' containing several numbers.

    <hide>
    >>> frame = tc.to_frame([[1], [3], [1], [0], [2], [1], [4], [3]], [('numbers', int)])
    -etc-

    </hide>

    >>> frame.inspect()
    [#]  numbers
    ============
    [0]        1
    [1]        3
    [2]        1
    [3]        0
    [4]        2
    [5]        1
    [6]        4
    [7]        3

    >>> ecdf_frame = frame.ecdf('numbers')
    <progress>

    >>> ecdf_frame.inspect()
    [#]  numbers  numbers_ecdf
    ==========================
    [0]        0         0.125
    [1]        1           0.5
    [2]        2         0.625
    [3]        3         0.875
    [4]        4           1.0

    """
    return self._tc.to_frame(self._scala.ecdf(column))