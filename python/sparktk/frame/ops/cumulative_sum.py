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