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

def cumulative_percent(self, sample_col):
    """
    Add column to frame with cumulative percent.

    Parameters
    ----------

    :param sample_col: (str) The name of the column from which to compute the cumulative percent.

    A cumulative percent sum is computed by sequentially stepping through the rows,
    observing the column values and keeping track of the current percentage of the
    total sum accounted for at the current value.

    Notes
    -----
    This method applies only to columns containing numerical data.
    Although this method will execute for columns containing negative
    values, the interpretation of the result will change (for example,
    negative percentages).

    Examples
    --------
    Consider Frame *my_frame* accessing a frame that contains a single
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

    The cumulative percent sum for column *obs* is obtained by:

        >>> my_frame.cumulative_percent('obs')
        <progress>

    The Frame *my_frame* now contains two columns *obs* and
    *obsCumulativePercentSum*.
    They contain the original data and the cumulative percent sum,
    respectively:

        >>> my_frame.inspect()
        [#]  obs  obs_cumulative_percent
        ================================
        [0]    0                     0.0
        [1]    1          0.166666666667
        [2]    2                     0.5
        [3]    0                     0.5
        [4]    1          0.666666666667
        [5]    2                     1.0


    """
    self._scala.cumulativePercent(sample_col)