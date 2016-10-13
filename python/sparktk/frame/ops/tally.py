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

def tally(self, sample_col, count_val):
    """
    Count number of times a value is seen.

    Parameters
    ----------

    :param sample_col: (str) The name of the column from which to compute the cumulative count.
    :param count_val: (str) The column value to be used for the counts.

    A cumulative count is computed by sequentially stepping through the rows,
    observing the column values and keeping track of the number of times the specified
    *count_value* has been seen.

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

        >>> my_frame.tally("obs", "1")
        <progress>

    The Frame *my_frame* accesses the original frame that now contains two
    columns, *obs* that contains the original column values, and
    *obsCumulativePercentCount* that contains the cumulative percent count:

        >>> my_frame.inspect()
        [#]  obs  obs_tally
        ===================
        [0]    0        0.0
        [1]    1        1.0
        [2]    2        1.0
        [3]    0        1.0
        [4]    1        2.0
        [5]    2        2.0

    """
    self._scala.tally(sample_col, count_val)