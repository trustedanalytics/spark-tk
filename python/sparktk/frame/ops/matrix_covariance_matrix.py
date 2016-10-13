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

def matrix_covariance_matrix(self, matrix_column_name):

    """

    Compute the Covariance Matrix of matrices stored in a frame

    Parameters
    ----------

    :param matrix_column_name: Name of the column to compute the covariance matrix on
    :return: (Frame) returns the frame with a new column storing the covariance matrix for the corresponding matrix

    Calculate the covariance matrix for each matrix in column 'matrix_column_name' of a frame using the following:

    Element (i,j) of the covariance matrix for a given matrix X is computed as: ((Xi - Mi)(Xj - Mj))
    where Mi is the mean

    Examples
    --------
        >>> from sparktk import dtypes
        >>> data = [[1, [[1,2,3,5],[2,3,5,6],[4,6,7,3],[8,9,2,4]]]]
        >>> schema = [('id', int),('pixeldata', dtypes.matrix)]
        >>> my_frame = tc.frame.create(data, schema)

        >>> my_frame.inspect()
        [#]  id  pixeldata
        ============================
        [0]   1  [[ 1.  2.  3.  5.]
        [ 2.  3.  5.  6.]
        [ 4.  6.  7.  3.]
        [ 8.  9.  2.  4.]]


        Compute the covariance matrix for the matrices in 'pixeldata' column of the frame
        >>> my_frame.matrix_covariance_matrix('pixeldata')

        A new column gets added to the existing frame storing the covariance matrix
        >>> my_frame.inspect()
        [#]  id  pixeldata
        ============================
        [0]   1  [[ 1.  2.  3.  5.]
        [ 2.  3.  5.  6.]
        [ 4.  6.  7.  3.]
        [ 8.  9.  2.  4.]]
        <BLANKLINE>
        [#]  CovarianceMatrix_pixeldata
        ============================================================
        [0]  [[  2.91666667   3.          -1.          -3.75      ]
        [  3.           3.33333333  -0.33333333  -5.        ]
        [ -1.          -0.33333333   3.33333333  -1.        ]
        [ -3.75        -5.          -1.          10.91666667]]

    """

    self._scala.matrixCovarianceMatrix(matrix_column_name)