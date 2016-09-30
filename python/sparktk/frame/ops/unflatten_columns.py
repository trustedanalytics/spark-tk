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


def unflatten_columns (self, columns, delimiter=","):
    """
    Compacts data from multiple rows based on cell data.

    Parameters
    ----------

    :param columns: (str or List[str]) Name of the column(s) to be used as keys for unflattening.
    :param delimiter: (Optional[str]) Separator for the data in the result columns.  Default is comma (,).

    Groups together cells in all columns (less the composite key) using "," as string delimiter.
    The original rows are deleted.
    The grouping takes place based on a composite key created from cell values.
    The column datatypes are changed to string.

    Examples
    --------

    <hide>
    >>> frame = tc.frame.create([["user1", "1/1/2015", 1, 70],["user1", "1/1/2015", 2, 60],["user2", "1/1/2015", 1, 65]],
    ...                         [('a', str), ('b', str),('c', int),('d', int)])
    <progress>

    </hide>

    Given a data file::

        user1 1/1/2015 1 70
        user1 1/1/2015 2 60
        user2 1/1/2015 1 65

    The commands to bring the data into a frame, where it can be worked on:

        >>> frame.inspect()
        [#]  a      b         c  d
        ===========================
        [0]  user1  1/1/2015  1  70
        [1]  user1  1/1/2015  2  60
        [2]  user2  1/1/2015  1  65


    Unflatten the data using columns a & b:

        >>> frame.unflatten_columns(['a','b'])
        <progress>

    Check again:

        >>> frame.inspect()
        [#]  a      b         c    d
        ================================
        [0]  user1  1/1/2015  1,2  70,60
        [1]  user2  1/1/2015  1    65

    Alternatively, unflatten_columns() also accepts a single column like:

    <hide>
    # Re-create frame with original data to start over with single column example
    >>> frame = tc.frame.create([["user1", "1/1/2015", 1, 70],["user1", "1/1/2015", 2, 60],["user2", "1/1/2015", 1, 65]],
    ...                         [('a', str), ('b', str),('c', int) ,('d', int)])
    <progress>

    </hide>

        >>> frame.unflatten_columns('a')
        <progress>

        >>> frame.inspect()
        [#]  a      b                  c    d
        =========================================
        [0]  user1  1/1/2015,1/1/2015  1,2  70,60
        [1]  user2  1/1/2015           1    65

    """
    if not isinstance(columns, list):
        columns = [columns]
    return self._scala.unflattenColumns(self._tc.jutils.convert.to_scala_list_string(columns),
                                        delimiter)