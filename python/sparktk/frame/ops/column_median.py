
def column_median(self, data_column, weights_column=None):
    """
    Calculate the (weighted) median of a column.

    :param data_column: The column whose median is to be calculated.
    :param weights_column: The column that provides weights (frequencies) for the median calculation.
                           Must contain numerical data.
                           Default is all items have a weight of 1.
    :return: varies
             The median of the values.
             If a weight column is provided and no weights are finite numbers greater
             than 0, None is returned.
             The type of the median returned is the same as the contents of the data
             column, so a column of Longs will result in a Long median and a column of
             Floats will result in a Float median.

    The median is the least value X in the range of the distribution so that
    the cumulative weight of values strictly below X is strictly less than half
    of the total weight and the cumulative weight of values up to and including X
    is greater than or equal to one-half of the total weight.

    All data elements of weight less than or equal to 0 are excluded from the
    calculation, as are all data elements whose weight is NaN or infinite.
    If a weight column is provided and no weights are finite numbers greater
    than 0, None is returned.

    Examples
    --------
    Given a frame with column 'a' accessed by a Frame object 'my_frame':

    .. code::
       >>> data = [[2],[3],[3],[5],[7],[10],[30]]
       >>> schema = [('a', int)]
       >>> my_frame = tc.to_frame(data, schema)
       <progress>

    Inspect my_frame

    .. code::

       >>> my_frame.inspect()
       [#]  a
       =======
       [0]   2
       [1]   3
       [2]   3
       [3]   5
       [4]   7
       [5]  10
       [6]  30

    Compute and return middle number of values in column *a*:

    .. code::

       >>> median = my_frame.column_median('a')
       <progress>
       >>> print median
       5

    Given a frame with column 'a' and column 'w' as weights accessed by a Frame object 'my_frame':

    .. code::

       >>> data = [[2,1.7],[3,0.5],[3,1.2],[5,0.8],[7,1.1],[10,0.8],[30,0.1]]
       >>> schema = [('a', int), ('w', float)]
       >>> my_frame = tc.to_frame(data, schema)
       <progress>

    Inspect my_frame

    .. code::

       >>> my_frame.inspect()
       [#]  a   w
        ============
        [0]   2  1.7
        [1]   3  0.5
        [2]   3  1.2
        [3]   5  0.8
        [4]   7  1.1
        [5]  10  0.8
        [6]  30  0.1

    Compute and return middle number of values in column 'a' with weights 'w':

    .. code::

       >>> median = my_frame.column_median('a', weights_column='w')
       <progress>
       >>> print median
       3

    """
    return self._scala.columnMedian(data_column, self._tc.jutils.convert.to_scala_option(weights_column)).value()