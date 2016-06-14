def join_right(self,
              right,
              left_on,
              right_on=None,
              use_broadcast_left=False):
    """
    join_right performs right join(right outer) operation on one or two frames, creating a new frame.


    Parameters
    ----------

    :param right: (Frame) Another frame to join with
    :param left_on: (List[str]) Names of the columns in the left frame used to match up the two frames.
    :param right_on: (Optional[List[str]])Names of the columns in the right frame used to match up the two frames. Default is the same as the left frame.
    :param use_broadcast_left: (bool) If left table is small enough to fit in the memory of a single machine,
            you can set use_broadcast_left to True to possibly improve performance using broadcast join. Default is False.

    :returns: (Frame) A new frame with the results of the join

    Create a new frame from a SQL JOIN operation with another frame.
    The frame on the 'left' is the currently active frame.
    The frame on the 'right' is another frame.
    This method take column(s) in the left frame and matches its values
    with column(s) in the right frame.
    'right' join works similarly to join_left, except it keeps all the data
    from the right frame and only the data from the left frame when it
    matches.

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

    Examples
    --------

    <hide>

    >>> codes = tc.frame.create([[1], [3], [1], [0], [2], [1], [5], [3]], [('numbers', int)])
    -etc-

    >>> colors = tc.frame.create([[1, 'red'], [2, 'yellow'], [3, 'green'], [4, 'blue']], [('numbers', int), ('color', str)])
    -etc-

    >>> country_code_rows = [[1, 354, "a"],[2, 91, "a"],[2, 100, "b"],[3, 47, "a"],[4, 968, "c"],[5, 50, "c"]]
    >>> country_code_schema = [("country_code", int),("area_code", int),("test_str",str)]
    -etc-

    >>> country_name_rows = [[1, "Iceland", "a"],[1, "Ice-land", "a"],[2, "India", "b"],[3, "Norway", "a"],[4, "Oman", "c"],[6, "Germany", "c"]]
    >>> country_names_schema = [("country_code", int),("country_name", str),("test_str",str)]
    -etc-

    >>> country_codes_frame = tc.frame.create(country_code_rows, country_code_schema)
    -etc-

    >>> country_names_frame= tc.frame.create(country_name_rows, country_names_schema)
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

    >>> j_right = codes.join_right(colors, 'numbers')
    <progress>

    >>> j_right.inspect()
    [#]  numbers_R  color
    ======================
    [0]          1  red
    [1]          1  red
    [2]          1  red
    [3]          2  yellow
    [4]          3  green
    [5]          3  green
    [6]          4  blue


    (The join adds an extra column *_R which is the join column from the right frame; it may be disregarded)

    Consider two frames: country_codes_frame and country_names_frame

    >>> country_codes_frame.inspect()
    [#]  country_code  area_code  test_str
    ======================================
    [0]             1        354  a
    [1]             2         91  a
    [2]             2        100  b
    [3]             3         47  a
    [4]             4        968  c
    [5]             5         50  c


    >>> country_names_frame.inspect()
    [#]  country_code  country_name  test_str
    =========================================
    [0]             1  Iceland       a
    [1]             1  Ice-land      a
    [2]             2  India         b
    [3]             3  Norway        a
    [4]             4  Oman          c
    [5]             6  Germany       c

    Join them on the 'country_code' and 'test_str' columns ('inner' join by default)

    >>> composite_join_right = country_codes_frame.join_right(country_names_frame, ['country_code', 'test_str'])
    <progress>

    >>> composite_join_right.inspect()
    [#]  area_code  country_code_R  country_name  test_str_R
    ========================================================
    [0]       None               6  Germany       c
    [1]        354               1  Iceland       a
    [2]        354               1  Ice-land      a
    [3]        100               2  India         b
    [4]         47               3  Norway        a
    [5]        968               4  Oman          c

    Right join broadcasting left table

    >>> j_right = codes.join_right(colors, 'numbers', use_broadcast_left=True)
    <progress>

    >>> j_right.inspect()
    [#]  numbers_R  color
    ======================
    [0]          1  red
    [1]          1  red
    [2]          1  red
    [3]          2  yellow
    [4]          3  green
    [5]          3  green
    [6]          4  blue

    >>> composite_join_right = country_codes_frame.join_right(country_names_frame, ['country_code', 'test_str'], use_broadcast_left=True)
    <progress>

    >>> composite_join_right.inspect()
    [#]  area_code  country_code_R  country_name  test_str_R
    ========================================================
    [0]        354               1  Iceland       a
    [1]        354               1  Ice-land      a
    [2]        100               2  India         b
    [3]         47               3  Norway        a
    [4]        968               4  Oman          c
    [5]       None               6  Germany       c

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

    return self._tc.frame.create(self._scala.joinRight(right._scala,
                                                  self._tc.jutils.convert.to_scala_list_string(left_on),
                                                  self._tc.jutils.convert.to_scala_option(
                                                          self._tc.jutils.convert.to_scala_list_string(right_on)),
                                                  use_broadcast_left))
