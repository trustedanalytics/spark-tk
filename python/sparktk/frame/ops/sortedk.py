
def sorted_k(self, k, column_names_and_ascending, reduce_tree_depth = 2):
    """
    Get a sorted subset of the data.

    Parameters
    ----------

    :param k: (int) Number of sorted records to return.
    :param column_names_and_ascending: (List[tuple(str, bool)]) Column names to sort by, and true to sort column
                                       by ascending order, or false for descending order.
    :param reduce_tree_depth: (int) Advanced tuning parameter which determines the depth of the
                              reduce-tree (uses Spark's treeReduce() for scalability.)
                              Default is 2.
    :return: (Frame) A new frame with a subset of sorted rows from the original frame.

    Take a number of rows and return them sorted in either ascending or descending order.

    Sorting a subset of rows is more efficient than sorting the entire frame when
    the number of sorted rows is much less than the total number of rows in the frame.

    Notes
    -----

    The number of sorted rows should be much smaller than the number of rows
    in the original frame.

    In particular:

    1.  The number of sorted rows returned should fit in Spark driver memory.
        The maximum size of serialized results that can fit in the Spark driver is
        set by the Spark configuration parameter *spark.driver.maxResultSize*.
    +   If you encounter a Kryo buffer overflow exception, increase the Spark
        configuration parameter *spark.kryoserializer.buffer.max.mb*.
    +   Use Frame.sort() instead if the number of sorted rows is very large (in
        other words, it cannot fit in Spark driver memory).

    Examples
    --------

    These examples deal with the most recently-released movies in a private collection.
    Consider the movie collection already stored in the frame below:

    <hide>
        >>> s = [("genre", str), ("year", int), ("title", str)]
        >>> rows = [["Drama", 1957, "12 Angry Men"], ["Crime", 1946, "The Big Sleep"], ["Western", 1969, "Butch Cassidy and the Sundance Kid"], ["Drama", 1971, "A Clockwork Orange"], ["Drama", 2008, "The Dark Knight"], ["Animation", 2013, "Frozen"], ["Drama", 1972, "The Godfather"], ["Animation", 1994, "The Lion King"], ["Animation", 2010, "Tangled"], ["Fantasy", 1939, "The WOnderful Wizard of Oz"]  ]
        >>> my_frame = tc.frame.create(rows, s)
        -etc-

    </hide>

        >>> my_frame.inspect()
        [#]  genre      year  title
        ========================================================
        [0]  Drama      1957  12 Angry Men
        [1]  Crime      1946  The Big Sleep
        [2]  Western    1969  Butch Cassidy and the Sundance Kid
        [3]  Drama      1971  A Clockwork Orange
        [4]  Drama      2008  The Dark Knight
        [5]  Animation  2013  Frozen
        [6]  Drama      1972  The Godfather
        [7]  Animation  1994  The Lion King
        [8]  Animation  2010  Tangled
        [9]  Fantasy    1939  The WOnderful Wizard of Oz


    This example returns the top 3 rows sorted by a single column: 'year' descending:

        >>> topk_frame = my_frame.sorted_k(3, [ ('year', False) ])
        <progress>

        >>> topk_frame.inspect()
        [#]  genre      year  title
        =====================================
        [0]  Animation  2013  Frozen
        [1]  Animation  2010  Tangled
        [2]  Drama      2008  The Dark Knight

    This example returns the top 5 rows sorted by multiple columns: 'genre' ascending, then 'year' descending:

        >>> topk_frame = my_frame.sorted_k(5, [ ('genre', True), ('year', False) ])
        <progress>

        >>> topk_frame.inspect()
        [#]  genre      year  title
        =====================================
        [0]  Animation  2013  Frozen
        [1]  Animation  2010  Tangled
        [2]  Animation  1994  The Lion King
        [3]  Crime      1946  The Big Sleep
        [4]  Drama      2008  The Dark Knight

    This example returns the top 5 rows sorted by multiple columns: 'genre'
    ascending, then 'year' ascending.
    It also illustrates the optional tuning parameter for reduce-tree depth
    (which does not affect the final result).

        >>> topk_frame = my_frame.sorted_k(5, [ ('genre', True), ('year', True) ], reduce_tree_depth=1)
        <progress>

        >>> topk_frame.inspect()
        [#]  genre      year  title
        ===================================
        [0]  Animation  1994  The Lion King
        [1]  Animation  2010  Tangled
        [2]  Animation  2013  Frozen
        [3]  Crime      1946  The Big Sleep
        [4]  Drama      1957  12 Angry Men

    """
    return self._tc.frame.create(self._scala.sortedK(k,
                                 self._tc.jutils.convert.to_scala_list_string_bool_tuple(column_names_and_ascending),
                                 reduce_tree_depth))