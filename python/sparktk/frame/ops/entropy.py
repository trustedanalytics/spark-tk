
def entropy(self, data_column, weights_column=None):
    """
    Calculate the Shannon entropy of a column.

    :param data_column: The column whose entropy is to be calculated.
    :param weights_column: The column that provides weights (frequencies) for the entropy calculation.
                           Must contain numerical data. Default is using uniform weights of 1 for all items.
    :return: Entropy.

    The data column is weighted via the weights column.
    All data elements of weight <= 0 are excluded from the calculation, as are
    all data elements whose weight is NaN or infinite.
    If there are no data elements with a finite weight greater than 0,
    the entropy is zero.

    Examples
    --------

    Consider the following sample data set in *frame* 'frame' containing several numbers.

    <hide>
    >>> frame = tc.to_frame([[0,1], [1,2], [2,4], [4,8]], [('data', int), ('weight', int)])
    -etc-

    </hide>

    >>> frame.inspect()
    [#]  data  weight
    =================
    [0]     0       1
    [1]     1       2
    [2]     2       4
    [3]     4       8
    >>> entropy = frame.entropy("data", "weight")
    <progress>

    >>> "%0.8f" % entropy
    '1.13691659'


    If we have more choices and weights, the computation is not as simple.
    An on-line search for "Shannon Entropy" will provide more detail.

    <hide>
    >>> frame = tc.to_frame([["H"], ["T"], ["H"], ["T"], ["H"], ["T"], ["H"], ["T"], ["H"], ["T"]], [('data', str)])
    -etc-

    </hide>
    Given a frame of coin flips, half heads and half tails, the entropy is simply ln(2):

    >>> frame.inspect()
    [#]  data
    =========
    [0]  H
    [1]  T
    [2]  H
    [3]  T
    [4]  H
    [5]  T
    [6]  H
    [7]  T
    [8]  H
    [9]  T
    >>> entropy = frame.entropy("data")
    <progress>
    >>> "%0.8f" % entropy
    '0.69314718'

    """
    return self._scala.entropy(data_column, self._tc.jutils.convert.to_scala_option(weights_column))