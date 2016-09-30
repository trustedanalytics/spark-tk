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

from sparktk.frame.row import Row

def count(self, where=None):
    """
    Counts all rows or all qualified rows.

    Parameters
    ----------

    :param where: (UDF) Optional function which evaluates a row to a boolean to determine if it should be counted
    :return: (int) Number of rows counted

    Counts all rows or all rows which meet criteria specified by a UDF predicate.

    Examples
    --------

        >>> frame = tc.frame.create([['Fred',39,16,'555-1234'],
        ...                          ['Susan',33,3,'555-0202'],
        ...                          ['Thurston',65,26,'555-4510'],
        ...                          ['Judy',44,14,'555-2183']],
        ...                         schema=[('name', str), ('age', int), ('tenure', int), ('phone', str)])

        >>> frame.inspect()
        [#]  name      age  tenure  phone
        ====================================
        [0]  Fred       39      16  555-1234
        [1]  Susan      33       3  555-0202
        [2]  Thurston   65      26  555-4510
        [3]  Judy       44      14  555-2183

        >>> frame.count()
        4

        >>> frame.count(lambda row: row.age > 35)
        3

    """
    if where:
        row = Row(self.schema)

        def count_where(r):
            row._set_data(r)
            return where(row)

        return self._python.rdd.filter(lambda r: count_where(r)).count()
    else:
        if self._is_scala:
            return int(self._scala.rowCount())
        return self.rdd.count()
