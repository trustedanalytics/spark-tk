
def covariance(self, column_a, column_b):
    """
    Calculate covariance for exactly two columns.

    Parameters
    ----------

    :param column_a: (str) The name of the column from which to compute the covariance.
    :param column_b: (str) The name of the column from which to compute the covariance.
    :return: (float) Covariance of the two columns.

    Notes
    -----
    This method applies only to columns containing numerical data.

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


    my_frame.covariance computes the covariance on the pair of columns provided.

        >>> my_frame.covariance("x1", "x2")
        -2.5

        >>> my_frame.covariance("x1", "x4")
        0.0

        >>> my_frame.covariance("x2", "x3")
        -1.5

    """

    return self._scala.covariance(column_a, column_b)
