def svd(self, matrix_column_name):

    """
    Compute the Singular Value Decomposition of a matrix

    :param: m*n dimensional Breeze DenseMatrix
    :return: m*m dimensional U matrix, Vector of singular values, n*n dimensional V' matrix
    """

    self._scala.svd(matrix_column_name)