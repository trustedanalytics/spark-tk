def dot_product(self, left_column_names,right_column_names,dot_product_column_name,default_left_values=None,default_right_values=None):
    """
    Add column to frame with dot_product.

    :param left_column_names :  Names of columns used to create the left vector (A) for each row.
                                Names should refer to a single column of type vector, or two or more columns of numeric scalars.
    :param right_column_names: Names of columns used to create right vector (B) for each row.
                               Names should refer to a single column of type vector, or two or more columns of numeric scalars.
    :param dot_product_column_name: Name of column used to store the dot product.
    :param default_left_values: Default values used to substitute null values in left vector.Default is None.
    :param default_right_values: Default values used to substitute null values in right vector.Default is None.

    :return returns a frame with give "dot_product" column name

    Notes
    -----
    This method applies only to columns containing numerical data.

    Examples
    --------
        >>> data = [[1, 0.2, -2, 5], [2, 0.4, -1, 6], [3, 0.6, 0, 7], [4, 0.8, 1, 8]]
        >>> schema = [('col_0', int), ('col_1', float),('col_2', int) ,('col_3', int)]

        >>> my_frame = tc.to_frame(data, schema)
        <progress>

    Calculate the dot product for a sequence of columns in Frame object *my_frame*:

    .. code::

        >>> my_frame.inspect()
        [#]  col_0  col_1  col_2  col_3
        ===============================
        [0]      1    0.2     -2      5
        [1]      2    0.4     -1      6
        [2]      3    0.6      0      7
        [3]      4    0.8      1      8


    Modify the frame by computing the dot product for a sequence of columns:

    .. code::

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
