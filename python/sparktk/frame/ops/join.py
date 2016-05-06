def join(self,
         right,
         left_on,
         right_on=None,
         how='inner',
         name=None,
         useBroadcast=None):
    """
    Join operation on one or two frames, creating a new frame.

    @:param right : Another frame to join with
    @:param left_on :Names of the columns in the left frame used to match up the two frames.
    @:param right_on :Names of the columns in the right frame used to match up the two frames. Default is the same as the left frame.
    @:param how :How to qualify the data to be joined together. Must be one of the following: (left, right, inner or outer).Default is inner
    @:param name :Name of the result grouped frame
    @:param useBroadcast: If your tables are smaller broadcast is used for join. Specify which table is smaller(left or right). Defualt is None.

    @:returns: A new frame with the results of the join

    Create a new frame from a SQL JOIN operation with another frame.
    The frame on the 'left' is the currently active frame.
    The frame on the 'right' is another frame.
    This method take column(s) in the left frame and matches its values
    with column(s) in the right frame.
    Using the default 'how' option ['inner'] will only allow data in the
    resultant frame if both the left and right frames have the same value
    in the matching column(s).
    Using the 'left' 'how' option will allow any data in the resultant
    frame if it exists in the left frame, but will allow any data from the
    right frame if it has a value in its column(s) which matches the value in
    the left frame column(s).
    Using the 'right' option works similarly, except it keeps all the data
    from the right frame and only the data from the left frame when it
    matches.
    The 'outer' option provides a frame with data from both frames where
    the left and right frames did not have the same value in the matching
    column(s).

    Notes
    -----
    When a column is named the same in both frames, it will result in two
    columns in the new frame.
    The column from the *left* frame (originally the current frame) will be
    copied and the column name will have the string "_L" added to it.
    The same thing will happen with the column from the *right* frame,
    except its name has the string "_R" appended. The order of columns
    after this method is called is not guaranteed.

    It is recommended that you rename the columns to meaningful terms prior
    to using the ``join`` method.
    Keep in mind that unicode in column names will likely cause the
    drop_frames() method (and others) to fail!

    Examples
    --------

    <hide>

    >>> codes = tc.to_frame([[1], [3], [1], [0], [2], [1], [5], [3]], [('numbers', int)])
    -etc-

    >>> colors = tc.to_frame([[1, 'red'], [2, 'yellow'], [3, 'green'], [4, 'blue']], [('numbers', int), ('color', str)])
    -etc-

    >>> country_code_rows = [[1, 354, "a"],[2, 91, "a"],[2, 100, "b"],[3, 47, "a"],[4, 968, "c"],[5, 50, "c"]]
    >>> country_code_schema = [("col_0", int),("col_1", int),("col_2",str)]
    -etc-

    >>> country_name_rows = [[1, "Iceland", "a"],[1, "Ice-land", "a"],[2, "India", "b"],[3, "Norway", "a"],[4, "Oman", "c"],[6, "Germany", "c"]]
    >>> country_names_schema = [("col_0", int),("col_1", str),("col_2",str)]
    -etc-

    >>> country_codes_frame = tc.to_frame(country_code_rows, country_code_schema)
    -etc-

    >>> country_names_frame= tc.to_frame(country_name_rows, country_names_schema)
    -etc-

    </hide>

    Consider two frames: codes and colors

    >>> codes.inspect()
    [#]  numbers
    ============
    [0]        1
    [1]        3
    [2]        1
    [3]        0
    [4]        2
    [5]        1
    [6]        5
    [7]        3


    >>> colors.inspect()
    [#]  numbers  color
    ====================
    [0]        1  red
    [1]        2  yellow
    [2]        3  green
    [3]        4  blue


    Join them on the 'numbers' column ('inner' join by default)

    >>> j = codes.join(colors, 'numbers')
    <progress>

    >>> j.inspect()
    [#]  numbers  color
    ====================
    [0]        1  red
    [1]        1  red
    [2]        1  red
    [3]        2  yellow
    [4]        3  green
    [5]        3  green

    (The join adds an extra column *_R which is the join column from the right frame; it may be disregarded)

    Try a 'left' join, which includes all the rows of the codes frame.

    >>> j_left = codes.join(colors, 'numbers', how='left')
    <progress>

    >>> j_left.inspect()
    [#]  numbers_L  color
    ======================
    [0]          0  None
    [1]          1  red
    [2]          1  red
    [3]          1  red
    [4]          2  yellow
    [5]          3  green
    [6]          3  green
    [7]          5  None


    And an outer join:

    >>> j_outer = codes.join(colors, 'numbers', how='outer')
    <progress>

    >>> j_outer.inspect()
    [#]  numbers_L  color
    ======================
    [0]          0  None
    [1]          1  red
    [2]          1  red
    [3]          1  red
    [4]          2  yellow
    [5]          3  green
    [6]          3  green
    [7]          4  blue
    [8]          5  None

    Consider two frames: country_codes_frame and country_names_frame

    >>> country_codes_frame.inspect()
    [#]  col_0  col_1  col_2
    ========================
    [0]      1    354  a
    [1]      2     91  a
    [2]      2    100  b
    [3]      3     47  a
    [4]      4    968  c
    [5]      5     50  c


    >>> country_names_frame.inspect()
    [#]  col_0  col_1     col_2
    ===========================
    [0]      1  Iceland   a
    [1]      1  Ice-land  a
    [2]      2  India     b
    [3]      3  Norway    a
    [4]      4  Oman      c
    [5]      6  Germany   c

    Join them on the 'col_0' and 'col_2' columns ('inner' join by default)

    >>> composite_join = country_codes_frame.join(country_names_frame, ['col_0', 'col_2'])
    <progress>

    >>> composite_join.inspect()
    [#]  col_0  col_1_L  col_2  col_1_R
    ====================================
    [0]      1      354  a      Iceland
    [1]      1      354  a      Ice-land
    [2]      2      100  b      India
    [3]      3       47  a      Norway
    [4]      4      968  c      Oman


    More examples can be found in the :ref:`user manual
    <example_frame.join>`.
    """
    if left_on is None:
        raise ValueError("Please provide column name on which join should be performed")
    elif isinstance(left_on, basestring):
        left_on = [left_on]
    if right_on is None:
        right_on = left_on
    elif isinstance(right_on, basestring):
        right_on = [right_on]
    if len(left_on) != len(right_on):
        raise ValueError("Please provide equal number of join columns")

    return self._tc.to_frame(self._scala.join(right._scala,
                                              self._tc.jutils.convert.to_scala_list_string(left_on),
                                              self._tc.jutils.convert.to_scala_option(
                                                      self._tc.jutils.convert.to_scala_list_string(right_on)),
                                              how,
                                              self._tc.jutils.convert.to_scala_option(name),
                                              self._tc.jutils.convert.to_scala_option(useBroadcast)))
