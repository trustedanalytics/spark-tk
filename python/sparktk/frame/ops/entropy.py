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


def entropy(self, data_column, weights_column=None):
    """
    Calculate the Shannon entropy of a column.

    Parameters
    ----------

    :param data_column: (str) The column whose entropy is to be calculated.
    :param weights_column: (Optional[str]) The column that provides weights (frequencies) for the entropy calculation.
                           Must contain numerical data. Default is using uniform weights of 1 for all items.
    :return: (float) Entropy.

    The data column is weighted via the weights column.
    All data elements of weight <= 0 are excluded from the calculation, as are
    all data elements whose weight is NaN or infinite.
    If there are no data elements with a finite weight greater than 0,
    the entropy is zero.

    Examples
    --------

    Consider the following sample data set in *frame* 'frame' containing several numbers.

    <hide>
    >>> frame = tc.frame.create([[0,1], [1,2], [2,4], [4,8]], [('data', int), ('weight', int)])
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
    >>> frame = tc.frame.create([["H"], ["T"], ["H"], ["T"], ["H"], ["T"], ["H"], ["T"], ["H"], ["T"]], [('data', str)])
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