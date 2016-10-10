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

def dicom_covariance_matrix(self, matrix_column_name):

    """

    Compute the Covariance Matrix of matrices stored in a frame

    :param matrix_column_name: Name of the column to compute the covariance matrix on
    :return: (Frame) returns the farme with a new
    """

    self._scala.dicomCovarianceMatrix(matrix_column_name)