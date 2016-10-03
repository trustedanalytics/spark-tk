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

def drop_rows(self, predicate):
    """
    Erase any row in the current frame which qualifies.

    Parameters
    ----------
    :param predicate: (UDF) Function which evaluates a row to a boolean; rows that answer True are dropped from
                      the frame.

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

        >>> frame.drop_rows(lambda row: row.name[-1] == 'n')  # drop people whose name ends in 'n'

        >>> frame.inspect()
        [#]  name  age  tenure  phone
        ================================
        [0]  Fred   39      16  555-1234
        [1]  Judy   44      14  555-2183

    More information on a |UDF| can be found at :doc:`/ds_apir`.
    """
    row = Row(self.schema)

    def drop_rows_func(r):
        row._set_data(r)
        return not predicate(row)
    self._python.rdd = self._python.rdd.filter(drop_rows_func)

