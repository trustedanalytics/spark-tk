
def quantiles(self, column_name, quantiles):
    """
    Returns a new frame with Quantiles and their values.

    :param column_name: The column to calculate quantiles
    :param quantiles: What is being requested.
    :return: A new frame with two columns (float64): requested Quantiles and their respective values.

    Calculates quantiles on the given column.

    Examples
    --------
    <hide>

        >>> data = [[100],[250],[95],[179],[315],[660],[540],[420],[250],[335]]
        >>> schema = [('final_sale_price', int)]

        >>> my_frame = tc.to_frame(data, schema)
        <progress>

    </hide>
    Consider Frame *my_frame*, which accesses a frame that contains a single
    column *final_sale_price*:

    .. code::

        >>> my_frame.inspect()
        [#]  final_sale_price
        =====================
        [0]               100
        [1]               250
        [2]                95
        [3]               179
        [4]               315
        [5]               660
        [6]               540
        [7]               420
        [8]               250
        [9]               335

    To calculate 10th, 50th, and 100th quantile:

    .. code::

        >>> quantiles_frame = my_frame.quantiles('final_sale_price', [10, 50, 100])
        <progress>

    A new Frame containing the requested Quantiles and their respective values
    will be returned :

    .. code::

       >>> quantiles_frame.inspect()
       [#]  Quantiles  final_sale_price_QuantileValue
       ==============================================
       [0]       10.0                            95.0
       [1]       50.0                           250.0
       [2]      100.0                           660.0

    """

    return self._tc.to_frame(self._scala.quantiles(column_name, self._tc.jutils.convert.to_scala_list_double(quantiles)))
