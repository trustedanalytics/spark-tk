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

def pca(self, matrix_column_name, u_matrix_column_name):

    """
    Compute the Singular Value Decomposition of a matrix

    :param: m*n dimensional Breeze DenseMatrix
    :return: m*m dimensional U matrix, Vector of singular values, n*n dimensional V' matrix
    """

    self._scala.pca(matrix_column_name, u_matrix_column_name)