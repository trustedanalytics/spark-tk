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

def matrix_svd(self, matrix_column_name):

    """

    Compute the Singular Value Decomposition of a matrix

    Parameters
    ----------

    :param matrix_column_name: Name of the column to compute the svd on
    :return: (Frame) returns the frame with three new columns storing the U matrix, Vt matrix and Singular Vectors

    Calculate the Singular Value Decomposition for each matrix in column 'matrix_column_name'

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

        Compute the singular value decomposition for the matrices in 'pixeldata' column of the frame
        >>> my_frame.matrix_svd('pixeldata')

        Three new columns get added storing the U matrix, Vt matrix and Singular Vectors
        >>> my_frame.inspect()
        [#]  id  pixeldata
        ============================
        [0]   1  [[ 1.  2.  3.  5.]
         [ 2.  3.  5.  6.]
         [ 4.  6.  7.  3.]
         [ 8.  9.  2.  4.]]
        <BLANKLINE>
        [#]  U_pixeldata
        ========================================================
        [0]  [[-0.29128979 -0.43716238 -0.44530839  0.72507913]
         [-0.42474933 -0.55066945 -0.26749936 -0.66692972]
         [-0.55099141 -0.16785045  0.79986267  0.16868433]
         [-0.65661765  0.69099814 -0.30060644 -0.0317899 ]]
        <BLANKLINE>
        [#]  Vt_pixeldata
        ========================================================
        [0]  [[-0.47195872 -0.60780067 -0.44835972 -0.45476024]
         [ 0.50289367  0.40702574 -0.58469285 -0.48945099]
         [-0.05244699  0.11313693  0.65644993 -0.74399115]
         [-0.72222035  0.67239008 -0.16180641  0.01039344]]
        <BLANKLINE>
        [#]  SingularVectors_pixeldata
        ============================================================
        [0]  [[ 18.21704938   6.59797925   3.54086993   0.26080987]]

    """

    self._scala.matrixSvd(matrix_column_name)