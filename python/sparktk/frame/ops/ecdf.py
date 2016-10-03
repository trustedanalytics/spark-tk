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


def ecdf(self, column):
    """
    Builds new frame with columns for data and distribution.

    Parameters
    ----------

    :param column: (str) The name of the input column containing sample.
    :return: (Frame) A new Frame containing each distinct value in the sample and its corresponding ECDF value.

    Generates the :term:`empirical cumulative distribution` for the input column.

    Examples
    --------

    Consider the following sample data set in *frame* 'frame' containing several numbers.

    <hide>
        >>> frame = tc.frame.create([[1], [3], [1], [0], [2], [1], [4], [3]], [('numbers', int)])
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
    from sparktk.frame.frame import Frame
    return Frame(self._tc, self._scala.ecdf(column))