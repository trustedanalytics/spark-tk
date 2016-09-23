def reverse_box_cox(self, column_name, lambda_value=0.0):
    """
    Calculate the reverse box-cox transformation for each row on a given column_name of the current frame

    Parameters
    ----------

    :param column_name: Name of the column to perform the reverse transformation on
    :param lambda_value: Lambda power parameter. Default is 0.0
    :return: (Frame) returns a frame with a new column storing the reverse box-cox transformed value

    Calculate the reverse box-cox transformation for each row in column 'column_name' of a frame using the lambda_value.

    Reverse Box-cox transformation is computed by the following formula:

    reverse_box_cox = exp(boxcox); if lambda=0,
    reverse_box_cox = (lambda * boxcox + 1)^(1/lambda) ; else

    Examples
    --------
    #
    #     >>> data = [[7.7132064326674596, 2.81913279907],[0.207519493594015, -1.25365381375],[6.336482349262754, 2.46673638752],
    #                     [7.4880388253861181, 2.76469126003],[4.9850701230259045, 2.06401101556]]
    #
    #     >>> schema = [("input", float), ("input_lambda_0.3", float)]
    #
    #     >>> my_frame = tc.frame.create(data, schema)
    #     <progress>
    #     >>> my_frame.inspect()
    #     [#]  input           input_lambda_0.3
    #     =====================================
    #     [0]   7.71320643267     2.81913279907
    #     [1]  0.207519493594    -1.25365381375
    #     [2]   6.33648234926     2.46673638752
    #     [3]   7.48803882539     2.76469126003
    #     [4]   4.98507012303     2.06401101556
    #
    # Compute the reverse box-cox transformation on the 'input_lambda_0.3' column which stores the box-cox transformed
    # value on column 'input' with lambda 0.3
    #     >>> my_frame.reverse_box_cox('input_lambda_0.3',0.3)
    #
    # A new column gets added to the frame which stores the reverse box-cox transformation for each row.
    # This value is equal to the original vales in 'input' column
    #     >>> my_frame.inspect()
    #     [#]  input           input_lambda_0.3  input_lambda_0.3reverse_lambda_0.3
    #     =========================================================================
    #     [0]   7.71320643267     2.81913279907                       7.71320643267
    #     [1]  0.207519493594    -1.25365381375                      0.207519493594
    #     [2]   6.33648234926     2.46673638752                       6.33648234926
    #     [3]   7.48803882539     2.76469126003                       7.48803882539
    #     [4]   4.98507012303     2.06401101556                       4.98507012303
    """
    self._scala.reverseBoxCox(column_name, lambda_value)