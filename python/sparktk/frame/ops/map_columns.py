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
import sparktk.frame.schema as schema_helper


def map_columns(self, func, schema):
    """
    Create a new frame from the output of a UDF which over each row of the current frame.

    Notes
    -----

    1.  The row |UDF| ('func') must return a value in the same format as
        specified by the schema.

    Parameters
    ----------

    :param func: (UDF) Function which takes the values in the row and produces a value, or collection of values, for the new cell(s).
    :param schema: (List[(str,type)]) Schema for the column(s) being added.

    Examples
    --------

    Given our frame, let's create a new frame with the name and a column with how many years the person has been over 18

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

        >>> adult = frame.map_columns(lambda row: [row.name, row.age - 18], [('name', str), ('adult_years', int)])

        >>> adult.inspect()
        [#]  name      adult_years
        ==========================
        [0]  Fred               21
        [1]  Susan              15
        [2]  Thurston           47
        [3]  Judy               26


    Note that the function returns a list, and therefore the schema also needs to be a list.

    It is not necessary to use lambda syntax, any function will do, as long as it takes a single row argument.  We
    can also call other local functions within.

    (see also the 'add_columns' frame operation)
    """

    schema_helper.validate(schema)
    row = Row(self.schema)

    def map_columns_func(r):
        row._set_data(r)
        return func(row)
    if isinstance(schema, list):
        rdd = self._python.rdd.map(lambda r: map_columns_func(r))
    else:
        rdd = self._python.rdd.map(lambda r: [map_columns_func(r)])
    return self._tc.frame.create(rdd, schema)

