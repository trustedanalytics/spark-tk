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

from setup import tc, rm, get_sandbox_path

def test_load_csv_with_missing_values_infer_schema(tc):
    # Load frame with missing values, inferring the schema
    path = "../datasets/missing_values.csv"
    frame = tc.frame.import_csv(path, header=False, infer_schema=True)

    # Check row count
    assert(5 == frame.count())

    # Check expected values
    expected_value = [['1',   2,    None, 4,    5,    None],
                      ['1',   2,    3,    None, None, 2.5],
                      ['2',   1,    3,    4,    5,    None],
                      ['dog', 20,   30,   40,   50,   60.5],
                      ['',    None, 13,   14,   15,   16.5]]
    assert(expected_value == frame.take(frame.count()))
    assert(frame.schema == [('C0', str),('C1', int),('C2', int),('C3', int),('C4', int),('C5', float)])

def test_load_csv_with_missing_values_custom_schema(tc):
    path = "../datasets/missing_values.csv"
    # specify the schema
    schema = [("a", str), ("b", int), ("c", float), ("d", int), ("e", int), ("f", float)]
    frame = tc.frame.import_csv(path, schema=schema)

    # Check row count
    assert(5 == frame.count())

    # Check expected values
    expected_value = [['1',   2,    None, 4,    5,    None],
                      ['1',   2,    3.0,  None, None, 2.5],
                      ['2',   1,    3.0,  4,    5,    None],
                      ['dog', 20,   30.0, 40,   50,   60.5],
                      ['',    None, 13.0, 14,   15,   16.5]]
    assert(expected_value == frame.take(frame.count()))
    assert(frame.schema == schema)

def test_missing_values_add_column(tc):
    # Create frame with missing values using upload rows
    schema = [('a', int)]
    data = [[1],[4],[None],[None],[10],[None]]
    frame = tc.frame.create(data, schema)

    # Check that frame was correctly created
    assert(6, frame.count())
    assert(data, frame.take(frame.count()))

    # Define function that replaces missing values with zero
    def noneToZero(x):
        if x is None:
            return 0
        else:
            return x

    # Use add columns to create a new column that replaces missing values with 0.
    frame.add_columns(lambda row: noneToZero(row['a']), ('a_corrected', int))
    expected = [[1],[4],[0],[0],[10],[0]]
    assert(expected, frame.take(frame.count(), columns='a_corrected'))


def test_missing_values_drop_rows(tc):
    # Create frame with missing values using upload rows
    schema = [('a', int)]
    data = [[1],[4],[None],[None],[10],[None]]
    frame = tc.frame.create(data, schema)

    # Check that frame was correctly created
    assert(6 == frame.count())
    assert(data == frame.take(frame.count()))

    # Check that we can drop rows with missing values
    frame.drop_rows(lambda row: row['a'] == None)
    expected = [[1],[4],[10]]
    assert(expected, frame.take(frame.count(), columns='a'))


def test_missing_values_with_frame_create_infer_schema(tc):
    data = [[1],[4],[None],[None],[10],[None]]
    frame = tc.frame.create(data)
    assert(len(frame.schema) == 1)
    assert(frame.schema[0][1] == int)