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


def correlation_matrix(self, data_column_names):
    """
    Calculate correlation matrix for two or more columns.

    Parameters
    ----------

    :param data_column_names: (List[str]) The names of the columns from which to compute the matrix.
    :return: (Frame) A Frame with the matrix of the correlation values for the columns.

    Notes
    -----
    This method applies only to columns containing numerical data.

    Examples
    --------
    Consider Frame *my_frame*, which contains the data

        <hide>
        >>> s = [("idnum", int), ("x1", float), ("x2", float), ("x3", float), ("x4", float)]
        >>> rows = [ [0, 1.0, 4.0, 0.0, -1.0], [1, 2.0, 3.0, 0.0, -1.0], [2, 3.0, 2.0, 1.0, -1.0], [3, 4.0, 1.0, 2.0, -1.0], [4, 5.0, 0.0, 2.0, -1.0]]
        >>> my_frame = tc.frame.create(rows, s)
        -etc-

        </hide>

        >>> my_frame.inspect()
         [#]  idnum  x1   x2   x3   x4
        ===============================
        [0]      0  1.0  4.0  0.0  -1.0
        [1]      1  2.0  3.0  0.0  -1.0
        [2]      2  3.0  2.0  1.0  -1.0
        [3]      3  4.0  1.0  2.0  -1.0
        [4]      4  5.0  0.0  2.0  -1.0


    my_frame.correlation_matrix computes the common correlation coefficient (Pearson's) on each pair
    of columns in the user-provided list.
    In this example, the *idnum* and most of the columns have trivial correlations: -1, 0, or +1.
    Column *x3* provides a contrasting coefficient of 3 / sqrt(3) = 0.948683298051

        >>> corr_matrix = my_frame.correlation_matrix(my_frame.column_names)
        <progress>

        The resulting table (specifying all columns) is:

        >>> corr_matrix.inspect()
        [#]  idnum           x1              x2               x3               x4
        ==========================================================================
        [0]             1.0             1.0             -1.0   0.948683298051  nan
        [1]             1.0             1.0             -1.0   0.948683298051  nan
        [2]            -1.0            -1.0              1.0  -0.948683298051  nan
        [3]  0.948683298051  0.948683298051  -0.948683298051              1.0  nan
        [4]             nan             nan              nan              nan  1.0

    """
    from sparktk.frame.frame import Frame
    return Frame(self._tc,
                 self._scala.correlationMatrix(self._tc.jutils.convert.to_scala_list_string(data_column_names)))
