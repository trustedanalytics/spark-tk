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


def reverse_box_cox(self, column_name, lambda_value=0.0, reverse_box_cox_column_name= None):
    """
    Calculate the reverse box-cox transformation for each row on a given column_name of the current frame

    Parameters
    ----------

    :param column_name: Name of the column to perform the reverse transformation on
    :param lambda_value: Lambda power parameter. Default is 0.0
    :param reverse_box_cox_column_name: Optional column name for the reverse box cox value
    :return: (Frame) returns a frame with a new column storing the reverse box-cox transformed value

    Calculate the reverse box-cox transformation for each row in column 'column_name' of a frame using the lambda_value.

    Reverse Box-cox transformation is computed by the following formula:

    reverse_box_cox = exp(boxcox); if lambda=0,
    reverse_box_cox = (lambda * boxcox + 1)^(1/lambda) ; else

    Examples
    --------

        >>> data = [[7.7132064326674596, 2.81913279907],[0.207519493594015, -1.25365381375],[6.336482349262754, 2.46673638752], [7.4880388253861181, 2.76469126003],[4.9850701230259045, 2.06401101556]]
        >>> schema = [("input", float), ("input_lambda_0.3", float)]
        >>> my_frame = tc.frame.create(data, schema)
        >>> my_frame.inspect()
        [#]  input           input_lambda_0.3
        =====================================
        [0]   7.71320643267     2.81913279907
        [1]  0.207519493594    -1.25365381375
        [2]   6.33648234926     2.46673638752
        [3]   7.48803882539     2.76469126003
        [4]   4.98507012303     2.06401101556

        Compute the reverse box-cox transformation on the 'input_lambda_0.3' column which stores the box-cox transformed
        value on column 'input' with lambda 0.3
        >>> my_frame.reverse_box_cox('input_lambda_0.3',0.3)

        A new column gets added to the frame which stores the reverse box-cox transformation for each row.
        This value is equal to the original vales in 'input' column
        >>> my_frame.inspect()
        [#]  input           input_lambda_0.3  input_lambda_0.3_reverse_lambda_0.3
        ==========================================================================
        [0]   7.71320643267     2.81913279907                        7.71320643267
        [1]  0.207519493594    -1.25365381375                       0.207519493594
        [2]   6.33648234926     2.46673638752                        6.33648234926
        [3]   7.48803882539     2.76469126003                         7.4880388254
        [4]   4.98507012303     2.06401101556                        4.98507012301

    """

    self._scala.reverseBoxCox(column_name, lambda_value, self._tc.jutils.convert.to_scala_option(reverse_box_cox_column_name))