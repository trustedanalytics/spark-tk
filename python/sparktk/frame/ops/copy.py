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
import types

def copy(self, columns=None, where=None):
    """
    New frame with copied columns.

    Parameters
    ----------

    :param columns: (str, List[str], or dictionary(str,str))  If not None, the copy will only include the
                    columns specified.  If dict, the string pairs represent a column renaming
                    { source_column_name : destination_column_name }
    :param where: (UDF) Optionally provide a where function.  If not None, only those rows for which the UDF
                  evaluates to True will be copied.
    :return: (Frame) New Frame object.

    Copies specified columns into a new Frame object, optionally renaming them and/or filtering them.
    Useful for frame query.

    Examples
    --------

    <hide>
    >>> schema = [("name", str), ("age", int), ("years", int)]
    >>> data = [["Thurston",64,26],["Judy",44,14],["Emily",37,5],["Frank",50,18],["Joe",43,11],["Ruth",52,21]]
    >>> frame = tc.frame.create(data, schema)
    </hide>

    Consider the following frame of employee names, age, and years of service:

        >>> frame.inspect()
        [#]  name      age  years
        =========================
        [0]  Thurston   64     26
        [1]  Judy       44     14
        [2]  Emily      37      5
        [3]  Frank      50     18
        [4]  Joe        43     11
        [5]  Ruth       52     21

        >>> frame.schema
        [('name', <type 'str'>), ('age', <type 'int'>), ('years', <type 'int'>)]

    To create a duplicate copy of the frame, use the copy operation with no parameters:

        >>> duplicate = frame.copy()
        <progress>

        >>> duplicate.inspect()
        [#]  name      age  years
        =========================
        [0]  Thurston   64     26
        [1]  Judy       44     14
        [2]  Emily      37      5
        [3]  Frank      50     18
        [4]  Joe        43     11
        [5]  Ruth       52     21

    Using the copy operation, we can also limit the new frame to just include the 'name' column:

        >>> names = frame.copy("name")
        <progress>

        >>> names.inspect()
        [#]  name
        =============
        [0]  Thurston
        [1]  Judy
        [2]  Emily
        [3]  Frank
        [4]  Joe
        [5]  Ruth

    We could also include a UDF to filter the data that is included in the new frame, and also provide
    a dictionary to rename the column(s) in the new frame.  Here we will use copy to create a frame of
    names for the employees that have over 20 years of service and also rename of the 'name' column to
    'first_name':

        >>> names = frame.copy({"name" : "first_name"}, lambda row: row.years > 20)
        <progress>

        >>> names.inspect()
        [#]  first_name
        ===============
        [0]  Thurston
        [1]  Ruth

    """
    new_rdd = self._python.rdd

    if where is not None and not isinstance(where, types.FunctionType):
        raise ValueError("Unsupported type for 'where' parameter.  Must be a function or None, but is: {0}".format(type(where)))

    if isinstance(columns, str):
        columns = [columns]
    if isinstance(columns, list):
        column_indices = [i for i, column in enumerate(self._python.schema) if column[0] in columns]
    elif isinstance(columns, dict):
        column_indices = [i for i, column in enumerate(self._python.schema) if column[0] in columns.keys()]
    elif columns is None:
        column_indices = xrange(0, len(self._python.schema))
    else:
        raise ValueError("Unsupported type for 'columns' parameter. Expected str, list, dict, or None, but was: {0}".format(type(columns)))

    if where is not None:
        # If a udf is provided, apply that function and apply the new schema
        row = Row(self._python.schema)
        def copy_func(r):
            row._set_data(r)
            return where(row)
        new_rdd = self._python.rdd.filter(lambda r: copy_func(r))
    if len(column_indices) < len(self._python.schema):
        # Map rows to only include the specified columns
        row = Row(self._python.schema)
        def map_func(r):
            row._set_data(r)
            return list(row[i] for i in column_indices)
        new_rdd = new_rdd.map(lambda r: map_func(r))

    new_schema = list(self._python.schema[i] for i in column_indices)

    # If columns are being renamed through a dictionary, alter the schema
    if (isinstance(columns, dict)):
        renamed_schema = []
        for column in new_schema:
            if columns.has_key(column[0]):
                new_name = columns[column[0]]
                renamed_schema.append((new_name, column[1]))
        new_schema = renamed_schema

    # return new frame with the filtered rdd and new schema
    return self._tc.frame.create(new_rdd, new_schema)

