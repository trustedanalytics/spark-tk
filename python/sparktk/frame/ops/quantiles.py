# vim: set encoding=utf-8

#  Copyright (c) 2016 Intel Corporation 
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#


def quantiles(self, column_name, quantiles):
    """
    Returns a new frame with Quantiles and their values.

    Parameters
    ----------

    :param column_name: (str) The column to calculate quantiles on
    :param quantiles: (List[float]) The quantiles being requested
    :return: (Frame) A new frame with two columns (float): requested Quantiles and their respective values.

    Calculates quantiles on the given column.

    Examples
    --------
    <hide>

        >>> data = [[100],[250],[95],[179],[315],[660],[540],[420],[250],[335]]
        >>> schema = [('final_sale_price', int)]

        >>> my_frame = tc.frame.create(data, schema)
        <progress>

    </hide>

    Consider Frame *my_frame*, which accesses a frame that contains a single
    column *final_sale_price*:

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

        >>> quantiles_frame = my_frame.quantiles('final_sale_price', [10, 50, 100])
        <progress>

    A new Frame containing the requested Quantiles and their respective values
    will be returned:

       >>> quantiles_frame.inspect()
       [#]  Quantiles  final_sale_price_QuantileValue
       ==============================================
       [0]       10.0                            95.0
       [1]       50.0                           250.0
       [2]      100.0                           660.0

    """
    from sparktk.frame.frame import Frame
    return Frame(self._tc, self._scala.quantiles(column_name, self._tc.jutils.convert.to_scala_list_double(quantiles)))
