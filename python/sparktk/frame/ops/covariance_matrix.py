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


def covariance_matrix(self, data_column_names):
    """
    Calculate covariance matrix for two or more columns.

    Parameters
    ----------

    :param data_column_names: (List[str]) The names of the column from which to compute the matrix.
                              Names should refer to a single column of type vector, or two or more
                              columns of numeric scalars.
    :return: (Frame) A matrix with the covariance values for the columns.

    Notes
    -----
    This function applies only to columns containing numerical data.

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


    my_frame.covariance_matrix computes the covariance on each pair of columns in the user-provided list.

        >>> cov_matrix = my_frame.covariance_matrix(my_frame.column_names)
        <progress>

        The resulting table (specifying all columns) is:

        >>> cov_matrix.inspect()
        [#]  idnum  x1    x2    x3    x4
        =================================
        [0]    2.5   2.5  -2.5   1.5  0.0
        [1]    2.5   2.5  -2.5   1.5  0.0
        [2]   -2.5  -2.5   2.5  -1.5  0.0
        [3]    1.5   1.5  -1.5   1.0  0.0
        [4]    0.0   0.0   0.0   0.0  0.0

    """
    from sparktk.frame.frame import Frame
    return Frame(self._tc,
                 self._scala.covarianceMatrix(self._tc.jutils.convert.to_scala_list_string(data_column_names)))
