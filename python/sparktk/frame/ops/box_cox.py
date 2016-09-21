def box_cox(self, column_name, lambda_value=0.0):
    """
    Calculate the box-cox transformation for each row on a given column of the
     current frame

    Parameters
    ----------

    :param column_name: Name of the column to perform the transformation on
    :param lambda_value: Lambda power parameter. Default is 0.0
    :return: (Frame) returns a frame with a new column storing the box-cox transformed value

    Calculate the box-cox transformation for each row in column 'column_name' of a frame using the lambda_value.

    Box-cox transformation is computed by the following formula:

    boxcox = log(y); if lambda=0,
    boxcox = (y^lambda -1)/lambda ; else
    where log is the natural log

    Examples
    --------

        >>> data = [[7.7132064326674596],[0.207519493594015],[6.336482349262754],[7.4880388253861181],[4.9850701230259045]]
        >>> schema = [("input", float)]

        >>> my_frame = tc.frame.create(data, schema)
        <progress>
        >>> my_frame.inspect()
        [#]  input
        ===================
        [0]   7.71320643267
        [1]  0.207519493594
        [2]   6.33648234926
        [3]   7.48803882539
        [4]   4.98507012303

        Compute the box-cox transformation on the 'input' column
        >>> my_frame.box_cox('input',0.3)

        A new column gets added to the frame which stores the box-cox transformation for each row
        >>> my_frame.inspect()
        [#]  input           input_lambda_0.3
        =====================================
        [0]   7.71320643267     2.81913279907
        [1]  0.207519493594    -1.25365381375
        [2]   6.33648234926     2.46673638752
        [3]   7.48803882539     2.76469126003
        [4]   4.98507012303     2.06401101556
    """
    self._scala.boxCox(column_name, lambda_value)