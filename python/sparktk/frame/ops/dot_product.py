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

def dot_product(self, left_column_names,right_column_names,dot_product_column_name,default_left_values=None,default_right_values=None):
    """
    Calculate dot product for each row in current frame.

    Parameters
    ----------

    :param left_column_names: (List[str]) Names of columns used to create the left vector (A) for each row.
                                Names should refer to a single column of type vector, or two or more columns of numeric scalars.
    :param right_column_names: (List[str]) Names of columns used to create right vector (B) for each row.
                               Names should refer to a single column of type vector, or two or more columns of numeric scalars.
    :param dot_product_column_name: (str) Name of column used to store the dot product.
    :param default_left_values: (Optional[List[float]) Default values used to substitute null values in left vector.Default is None.
    :param default_right_values: (Optional[List[float]) Default values used to substitute null values in right vector.Default is None.

    :return: (Frame) returns a frame with give "dot_product" column name

    Calculate the dot product for each row in a frame using values from two equal-length sequences of columns.

    Dot product is computed by the following formula:

    The dot product of two vectors :math:`A=[a_1, a_2, ..., a_n]` and :math:`B =[b_1, b_2, ..., b_n]` is :math:`a_1*b_1 + a_2*b_2 + ...+ a_n*b_n`.
    The dot product for each row is stored in a new column in the existing frame.

    Notes
    -----

    * If default_left_values or default_right_values are not specified, any null values will be replaced by zeros.
    * This method applies only to columns containing numerical data.


    Examples
    --------

        >>> data = [[1, 0.2, -2, 5], [2, 0.4, -1, 6], [3, 0.6, 0, 7], [4, 0.8, 1, 8]]
        >>> schema = [('col_0', int), ('col_1', float),('col_2', int) ,('col_3', int)]

        >>> my_frame = tc.frame.create(data, schema)
        <progress>

    Calculate the dot product for a sequence of columns in Frame object *my_frame*:

        >>> my_frame.inspect()
        [#]  col_0  col_1  col_2  col_3
        ===============================
        [0]      1    0.2     -2      5
        [1]      2    0.4     -1      6
        [2]      3    0.6      0      7
        [3]      4    0.8      1      8


    Modify the frame by computing the dot product for a sequence of columns:

         >>> my_frame.dot_product(['col_0','col_1'], ['col_2', 'col_3'], 'dot_product')
         <progress>

        >>> my_frame.inspect()
        [#]  col_0  col_1  col_2  col_3  dot_product
        ============================================
        [0]      1    0.2     -2      5         -1.0
        [1]      2    0.4     -1      6          0.4
        [2]      3    0.6      0      7          4.2
        [3]      4    0.8      1      8         10.4

    """

    if not isinstance(left_column_names, list):
        left_column_names = [left_column_names]
    if not isinstance(right_column_names, list):
        right_column_names = [right_column_names]
    self._scala.dotProduct(self._tc.jutils.convert.to_scala_list_string(left_column_names),
                           self._tc.jutils.convert.to_scala_list_string(right_column_names),
                           dot_product_column_name,
                           self._tc.jutils.convert.to_scala_option_list_double(default_left_values),
                           self._tc.jutils.convert.to_scala_option_list_double(default_right_values))
