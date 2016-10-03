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

def drop_duplicates(self, unique_columns=None):
    """
    Modify the current frame, removing duplicate rows.

    Parameters
    ----------

    :param unique_columns: (Optional[List[str] or str]) Column name(s) to identify duplicates. Default is the entire
                           row is compared.

    Remove data rows which are the same as other rows.
    The entire row can be checked for duplication, or the search for duplicates can be limited to one or more columns.
    This modifies the current frame.

    Examples
    --------

    Given a frame with data:

        <hide>
        >>> frame = tc.frame.create([[200, 4, 25],
        ...                          [200, 5, 25],
        ...                          [200, 4, 25],
        ...                          [200, 5, 35],
        ...                          [200, 6, 25],
        ...                          [200, 8, 35],
        ...                          [200, 4, 45],
        ...                          [200, 4, 25],
        ...                          [200, 5, 25],
        ...                          [201, 4, 25]],
        ...                         [("a", int), ("b", int), ("c", int)])
        <progress>

        </hide>

        >>> frame.inspect()
        [#]  a    b  c
        ===============
        [0]  200  4  25
        [1]  200  5  25
        [2]  200  4  25
        [3]  200  5  35
        [4]  200  6  25
        [5]  200  8  35
        [6]  200  4  45
        [7]  200  4  25
        [8]  200  5  25
        [9]  201  4  25

    Remove any rows that are identical to a previous row.
    The result is a frame of unique rows.
    Note that row order may change.

        >>> frame.drop_duplicates()
        <progress>

        >>> frame.inspect()
        [#]  a    b  c
        ===============
        [0]  200  8  35
        [1]  200  6  25
        [2]  200  5  35
        [3]  200  4  45
        [4]  200  4  25
        [5]  200  5  25
        [6]  201  4  25

    Now remove any rows that have the same data in columns *a* and
    *c* as a previously checked row:

        >>> frame.drop_duplicates([ "a", "c"])
        <progress>

    The result is a frame with unique values for the combination of columns *a*
    and *c*.

        >>> frame.inspect()
        [#]  a    b  c
        ===============
        [0]  201  4  25
        [1]  200  4  45
        [2]  200  6  25
        [3]  200  8  35

    """
    if unique_columns is not None and not isinstance(unique_columns, list):
        unique_columns = [unique_columns]
    if isinstance(unique_columns, list):
        unique_columns = self._tc.jutils.convert.to_scala_vector_string(unique_columns)
    self._scala.dropDuplicates(self._tc.jutils.convert.to_scala_option(unique_columns))