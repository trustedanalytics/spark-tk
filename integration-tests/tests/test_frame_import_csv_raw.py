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


def test_import_csv_raw(tc):
    """
    Tests import_csv_raw, where all columns are brought in as strings.
    """
    path = "../datasets/cities.csv"
    f = tc.frame.import_csv_raw(path, delimiter="|", header=True)
    assert(f.count() == 20)
    assert(len(f.schema) == 6)

    # Verify that all columns in the schema are strings
    assert(f.schema == [('rank', str),
                        ('city', str),
                        ('population_2013', str),
                        ('population_2010', str),
                        ('change', str),
                        ('county', str)])

    # Verify that data is strings
    for row in f.take(f.count()):
        for value in row:
            assert(isinstance(value, basestring))

def test_import_csv_raw_no_header(tc):
    """
    Tests import_csv_raw with a file that has no header row, to verify that columns are named generically.
    """
    path = "../datasets/noheader.csv"
    f = tc.frame.import_csv_raw(path, delimiter=",", header=False)
    assert(f.count() == 10)
    assert(len(f.schema) == 4)

    # Verify that all columns in the schema are named generically and have str types
    assert(f.schema == [('C0', str), ('C1', str), ('C2', str), ('C3', str)])

    # Verify that data is strings
    for row in f.take(f.count()):
        for value in row:
            assert(isinstance(value, basestring))

def test_invalid_delimiter(tc):
    """
    Tests for exceptions based on an invalid delimiter parameter
    """

    path = "../datasets/noheader.csv"

    try:
        tc.frame.import_csv_raw(path, delimiter="", header=False)
    except Exception as e:
        assert("Found delimiter = .  Expected non-empty string." in str(e))

    try:
        tc.frame.import_csv_raw(path, delimiter=5, header=False)
    except Exception as e:
        assert("Value for delimiter is of type <type 'int'>.  Expected type <type 'str'>." in str(e))

def test_invalid_header(tc):
    """
    Tests for exceptions based on an invalid header parameter
    """

    path = "../datasets/noheader.csv"

    try:
        tc.frame.import_csv_raw(path, header=5)
    except Exception as e:
        assert("Value for header is of type <type 'int'>.  Expected type <type 'bool'>." in str(e))

def test_invalid_path(tc):
    """
    Tests for exceptions based on an invalid path parameter
    """

    try:
        tc.frame.import_csv_raw("bogus.csv")
    except Exception as e:
        assert("InvalidInputException: Input path does not exist" in str(e))

    try:
        tc.frame.import_csv_raw("")
    except Exception as e:
        assert("Found path = .  Expected non-empty string." in str(e))

    try:
        tc.frame.import_csv_raw(path=5)
    except Exception as e:
        assert("Value for path is of type <type 'int'>.  Expected type <type 'str'>." in str(e))