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
from sparktk import dtypes

def test_import_csv(tc):
    path = "../datasets/importcsvtest.csv"
    # Test with inferred schema
    f = tc.frame.import_csv(path, header=True)
    assert(f.count() == 10)
    assert(len(f.schema) == 4)
    assert(f.schema == [("string_column", str),
                        ("integer_column", int),
                        ("float_column", float),
                        ("datetime_column", dtypes.datetime)])
    assert(f._is_python)
    # Make sure that we can go to scala (let's do that by binning the integer column)]
    f.bin_column("integer_column", [0,25,50,75,100])
    assert(len(f.schema) == 5)
    assert(f._is_scala)

def test_import_csv_with_custom_schema(tc):
    path = "../datasets/cities.csv"
    try:
        # Test with bad schema (incorrect number of columns)
        f = tc.frame.import_csv(path, "|", header=True, schema=[("a", int),("b", str)])
        f.take(f.count())
        raise RuntimeError("Expected SparkException from import_csv due to incorrect number of columns in custom schema.")
    except:
        pass

    # Test with good schema
    schema = [("a",int),("b",str),("c",int),("d",int),("e",str),("f",str)]
    f = tc.frame.import_csv(path, "|", header=True, schema=schema)
    assert(f.count() == 20)
    assert(f.schema == schema)

def test_import_csv_with_custom_schema_parse_error(tc):
    # Test with good schema, but bad values in file --bad values should render as None
    path = "../datasets/parse_error.csv"
    f = tc.frame.import_csv(path, schema=[("a", str),("b", int), ("c", float)], header=True)
    rows = f.take(f.count())
    assert(len(rows) == 4)
    assert(rows[2] == ["blue",100, None])         # bad float
    assert(rows[3] == ["purple",None, 3.33333])   # bad integer

def test_import_csv_with_no_header(tc):
    path = "../datasets/noheader.csv"
    # Test with no header and inferred schema
    f = tc.frame.import_csv(path, header=False)
    assert(f.count() == 10)
    assert(len(f.schema) == 4)
    assert(f.schema == [('C0', str), ('C1', int), ('C2', float), ('C3', dtypes.datetime)])

def test_import_csv_with_column_names(tc):
    path = "../datasets/noheader.csv"
    # Test with no header and inferred schema
    column_names = ["a","b","c","d"]
    f = tc.frame.import_csv(path, header=False, schema=column_names)
    assert(f.count() == 10)
    assert(len(f.schema) == 4)
    assert(f.schema == [('a', str), ('b', int), ('c', float), ('d', dtypes.datetime)])

def test_import_csv_with_invalid_header(tc):
    path = "../datasets/cities.csv"
    try:
        # Test with non-boolean header value
        tc.frame.import_csv(path, "|", header=5)
        raise RuntimeError("Expected TypeError from import_csv due to invalid (int) header parameter data type.")
    except TypeError as e:
        assert("Value for header is of type <type 'int'>.  Expected type <type 'bool'>." in str(e))

    try:
        # Test with non-boolean header value
        tc.frame.import_csv(path, "|", header="true")
        raise RuntimeError("Expected TypeError from import_csv due to invalid (string) header parameter data type.")
    except TypeError as e:
        assert("Value for header is of type <type 'str'>.  Expected type <type 'bool'>." in str(e))

def test_import_with_unsupported_type(tc):
    path = "../datasets/unsupported_types.csv"

    try:
        # Try creating a frame from a csv and infer the schema.  This csv has a boolean type, which we don't support,
        # so this should fail.
        tc.frame.import_csv(path, ",")
        raise RuntimeError("Expected TypeError for unsupported type found when inferring schema (boolean).")
    except TypeError:
        pass

    schema = [("id", int), ("name", str), ("bool", bool), ("day", str)]
    try:
        # Instead of inferring the schema, specify the schema that uses a boolean column.  This should still fail
        tc.frame.import_csv(path, ",", schema=schema)
        raise RuntimeError("Expected TypeError for unsupported type in the schema (bool).")
    except TypeError:
        pass

    schema = [("id", int), ("name", str), ("bool", str), ("day", str)]

    # Specify the boolean column as a string instead.  This should pass
    frame = tc.frame.import_csv(path, ",", schema=schema)
    assert(frame.count() == 5)
    assert(frame.schema == schema)

def test_frame_loading_multiple_files_with_wildcard(tc):
        frame = tc.frame.import_csv("../datasets/movie-part*.csv", header=True)
        assert(frame.schema == [('user', int),
                                ('vertex_type', str),
                                ('movie', int),
                                ('weight', int),
                                ('edge_type', str)])
        assert(frame.take(frame.count()) == [[1, 'L', -131, 0, 'tr'],
                                                    [-131, 'R', 1, 0, 'tr'],
                                                    [1, 'L', -300, 2, 'tr'],
                                                    [-300, 'R', 1, 2, 'tr'],
                                                    [1, 'L', -570, 4, 'tr'],
                                                    [-570, 'R', 1, 4, 'tr'],
                                                    [2, 'L', -778, 1, 'tr'],
                                                    [-778, 'R', 1, 1, 'tr'],
                                                    [1, 'L', -1209, 2, 'va'],
                                                    [-1209, 'R', 1, 2, 'va'],
                                                    [2, 'L', -1218, 3, 'tr'],
                                                    [-1218, 'R', 1, 3, 'tr'],
                                                    [1, 'L', -1232, 3, 'tr'],
                                                    [-1232, 'R', 1, 3, 'tr'],
                                                    [3, 'L', -1416, 2, 'tr'],
                                                    [-1416, 'R', 1, 2, 'tr'],
                                                    [3, 'L', -1648, 3, 'tr'],
                                                    [-1648, 'R', 1, 3, 'tr'],
                                                    [3, 'L', -2347, 3, 'tr'],
                                                    [-2347, 'R', 1, 3, 'tr']])
def test_import_csv_with_duplicate_coluns(tc):
    path = "../datasets/importcsvtest.csv"
    schema = [("string", str),
              ("numeric", int),
              ("numeric", float),
              ("datetime", dtypes.datetime)]
    try:
        # Try to create a frame from csv, using a schema that has duplicate column names
        tc.frame.import_csv(path, schema=schema, header=True)
    except Exception as e:
        assert("schema has duplicate column names: ['numeric']" in str(e))

def test_import_csv_datetime_format(tc):
    path = "../datasets/datetimes.csv"

    # Load with the date format that matches column a
    f = tc.frame.import_csv(path, schema=[("a",dtypes.datetime),("b",str)], datetime_format="yyyy-MM-ddX")

    expected = ["2015-01-03T00:00:00.000000Z","2015-04-12T00:00:00.000000Z"]
    actual_data = f.take(f.count())

    for row, expected_str in zip(actual_data, expected):
        assert(isinstance(row[0], long))    # 'a' datetime column should be a long (number of ms since epoch)
        assert(dtypes.ms_to_datetime_str(row[0]) == expected_str)
        assert(isinstance(row[1], basestring))     # column 'b' should be a str

    # Load with the date format that matches column b
    f = tc.frame.import_csv(path, schema=[("a",str),("b",dtypes.datetime)], datetime_format="MM-dd-yyyy kk:mm X")

    expected = ["2015-01-02T11:30:00.000000Z","2015-04-12T04:25:00.000000Z"]
    actual_data = f.take(f.count())

    for row, expected_str in zip(actual_data, expected):
        assert(isinstance(row[0], basestring))     # column 'a' should be a str
        assert(isinstance(row[1], long))    # column 'b' should be a long (number of ms since epoch)
        assert(dtypes.ms_to_datetime_str(row[1]) == expected_str)