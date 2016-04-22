from sparktk.propobj import PropertiesObject

class ColumnMode(PropertiesObject):
    """
    ColumnMode class that contains return values from the column_mode frame operation.
    """
    def __init__(self, column_mode_return):
        self._modes = list(column_mode_return.modes())
        self._mode_count = column_mode_return.modeCount()
        self._weight_of_mode = column_mode_return.weightOfMode()
        self._total_weight = column_mode_return.totalWeight()

    @property
    def mode_count(self):
        return self._mode_count

    @property
    def modes(self):
        return self._modes

    @property
    def total_weight(self):
        return self._total_weight

    @property
    def weight_of_mode(self):
        return self._weight_of_mode


def column_mode(self, data_column, weights_column=None, max_modes_returned=None):
    """
    Evaluate the weights assigned to rows.

    Calculate the modes of a column.
    A mode is a data element of maximum weight.
    All data elements of weight less than or equal to 0 are excluded from the
    calculation, as are all data elements whose weight is NaN or infinite.
    If there are no data elements of finite weight greater than 0,
    no mode is returned.

    Because data distributions often have multiple modes, it is possible for a
    set of modes to be returned.
    By default, only one is returned, but by setting the optional parameter
    max_modes_returned, a larger number of modes can be returned.

    :param data_column: Name of the column supplying the data.
    :param weights_column: Name of the column supplying the weights.
                           Default is all items have weight of 1.
    :param max_modes_returned: Maximum number of modes returned. Default is 1.
    :return: ColumnMode object containing summary statistics.
    The data returned is composed of multiple components\:

    mode : A mode is a data element of maximum net weight.
        A set of modes is returned.
        The empty set is returned when the sum of the weights is 0.
        If the number of modes is less than or equal to the parameter
        max_modes_returned, then all modes of the data are
        returned.
        If the number of modes is greater than the max_modes_returned
        parameter, only the first max_modes_returned many modes (per a
        canonical ordering) are returned.
    weight_of_mode : Weight of a mode.
        If there are no data elements of finite weight greater than 0,
        the weight of the mode is 0.
        If no weights column is given, this is the number of appearances
        of each mode.
    total_weight : Sum of all weights in the weight column.
        This is the row count if no weights are given.
        If no weights column is given, this is the number of rows in
        the table with non-zero weight.
    mode_count : The number of distinct modes in the data.
        In the case that the data is very multimodal, this number may
        exceed max_modes_returned.

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


    Compute and return a ColumnMode object containing summary statistics of column *a*:

    .. code::

       >>> mode = my_frame.column_mode('a')
       <progress>
       >>> print mode
        mode_count     = 1
        modes          = [3]
        total_weight   = 7.0
        weight_of_mode = 2.0

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

    Compute and return ColumnMode object containing summary statistics of column 'a' with weights 'w':

    .. code::

       >>> mode = my_frame.column_mode('a', weights_column='w')
       <progress>
       >>> print mode
        mode_count     = 2
        modes          = [2]
        total_weight   = 6.2
        weight_of_mode = 1.7

    """
    return ColumnMode(self._scala.columnMode(data_column,
                      self._tc.jutils.convert.to_scala_option(weights_column),
                      self._tc.jutils.convert.to_scala_option(max_modes_returned)))