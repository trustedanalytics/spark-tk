
def correlation(self, column_a, column_b):
    """
    Calculate correlation for two columns of current frame.

    :param column_a: The name of the column from which to compute the correlation.
    :param column_b: The name of the column from which to compute the correlation.
    :return: Pearson correlation coefficient of the two columns.

    Notes
    -----
    This method applies only to columns containing numerical data.

    Examples
    --------
    Consider Frame *my_frame*, which contains the data

        <hide>
        >>> s = [("idnum", int), ("x1", float), ("x2", float), ("x3", float), ("x4", float)]
        >>> rows = [ [0, 1.0, 4.0, 0.0, -1.0], [1, 2.0, 3.0, 0.0, -1.0], [2, 3.0, 2.0, 1.0, -1.0], [3, 4.0, 1.0, 2.0, -1.0], [4, 5.0, 0.0, 2.0, -1.0]]
        >>> my_frame = tc.to_frame(rows, s)
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


    my_frame.correlation computes the common correlation coefficient (Pearson's) on the pair
    of columns provided.
    In this example, the *idnum* and most of the columns have trivial correlations: -1, 0, or +1.
    Column *x3* provides a contrasting coefficient of 3 / sqrt(3) = 0.948683298051 .


        >>> my_frame.correlation("x1", "x2")
        <progress>

            -1.0
        >>> my_frame.correlation("x1", "x4")
        <progress>

            0.0
        >>> my_frame.correlation("x2", "x3")
        <progress>

            -0.948683298051
    """

    return self._scala.correlation(column_a, column_b)
