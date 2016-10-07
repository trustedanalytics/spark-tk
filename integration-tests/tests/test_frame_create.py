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

def invalid_schema_type(tc):
    """
    Verifies that an exception is thrown if a schema that's not a list or strings, list of tuples (str, type),
    or None is passed.
    """
    try:
        tc.frame.create([[1]], schema=7)
        raise RuntimeError("Expected TypeError when passing a schema that is just an integer")
    except TypeError:
        pass

    try:
        tc.frame.create([[1]], schema="test")
        raise RuntimeError("Expected TypeError when passing a schema that is just a string")
    except TypeError:
        pass

    try:
        tc.frame.create([[1, 2, 3]], schema=["col_a", "col_b", 1])
        raise RuntimeError("Expected TypeError when passing a schema is a list of strings and ints.")
    except TypeError:
        pass

    try:
        tc.frame.create([[1, 2, 3]], schema=[("col_a", str), ("col_b")])
        raise RuntimeError("Expected TypeError when passing a schema has incorrect tuple length.")
    except TypeError:
        pass

    try:
        tc.frame.create([[1, 2, 3]], schema=[("col_a", str), ("col_b", "str")])
        raise RuntimeError("Expected TypeError when passing a schema has incorrect tuple type.")
    except TypeError:
        pass

    try:
        tc.frame.create([[1,2,3]], schema=[("col0", int), ("col1", int), ("col0", int)])
        raise RuntimeError("Expected ValueError due to invalid schema")
    except ValueError as e:
        assert("Invalid schema, column names cannot be duplicated: col0" in e.message)

    try:
        tc.frame.create([[1,2,3]], schema=["col0", "col1", "col0"])
        raise RuntimeError("Expected ValueError due to invalid schema")
    except ValueError as e:
        assert("Invalid schema, column names cannot be duplicated: col0" in e.message)

def test_create_frame_with_column_names(tc):
    """
    Create a frame with a list of column names.  Data types should be inferred.
    """
    frame = tc.frame.create([[1,"a",1.5],[2,"b",5.0],[3,"c",22.7]],["number", "letter", "decimal"])
    assert(frame.count() == 3)
    assert(frame.schema == [("number", int), ("letter", str), ("decimal", float)])

def test_create_frame_without_schema(tc):
    """
    Creates a frame without a schema.  Column names should be numbered, and data types should be inferred.
    """
    frame = tc.frame.create([[1,"a",1.5],[2,"b",5.0],[3,"c",22.7]])
    assert(frame.count() == 3)
    assert(frame.schema == [("C0", int), ("C1", str), ("C2", float)])

def test_create_frame_with_not_enough_column_names(tc):
    """
    Creates a frame with a list of less column names than the amount of data.  The remaining column should
    be numbered.  Data types should be inferred.
    """
    frame = tc.frame.create([[1,"a",1.5],[2,"b",5.0],[3,"c",22.7]],["number", "letter"])
    assert(frame.count() == 3)
    assert(frame.schema == [("number", int), ("letter", str), ("C2", float)])

def test_create_frame_with_schema(tc):
    """
    Tests creating a frame with a custom schema
    """
    frame = tc.frame.create([[1,"a",1.5],[2,"b",5.0],[3,"c",22.7]], [("col_a", int),("col_b", str),("col_c", float)])
    assert(frame.count() == 3)
    assert(frame.schema == [("col_a", int), ("col_b", str), ("col_c", float)])

def test_create_frame_with_vectors(tc):
    """
    Tests  creating a frame with vectors, as well as failing use case where the vectors aren't all the same length.
    """
    # Create a frame with vectors
    frame = tc.frame.create([[[1,2,3,0]],[[0,4,5,6]],[[7,8,9,10]]])
    assert(frame.count() == 3)
    assert(len(frame.schema) == 1)
    assert(isinstance(frame.schema[0][1], dtypes.vector))   # schema data type should be a vector
    assert(frame.schema[0][1].length == 4)                  # vector length should be 4
    try:
        # Try to create a frame where vectors aren't all the same length
        tc.frame.create([[[1,2,3]],[[4,5,6]],[[7,8,9,10]]])
        raise RuntimeError("Expected exception when creating frame with different vector lengths.")
    except:
        pass

def test_create_with_schema_validation(tc):
    """
    Checks use cases with schema validation enabled.  Checks  use cases where we have all integers or a mix of integers
    and floats in the first 100 rows.  Verifies that in the case of mixed integers and floats, the integers should be
    casted to floats.  Checks for missing values in the use case where we have 100 integers followed by several strings
    that cannot be casted to integers.
    """
    # Test use case where we have more than 100 rows, all integers
    data = [[i] for i in xrange(0,110)]
    frame = tc.frame.create(data, validate_schema=True)
    row_count = frame.count()
    assert(row_count == 110)
    assert(frame.schema == [("C0", int)])
    assert(len(frame.take(row_count)) == frame.count())
    # Test use case where we have more than 100 rows, with a mix of integers and floats
    data = [[i] for i in xrange(0,55)] + [[i + .5] for i in xrange(0,55)]
    frame = tc.frame.create(data, validate_schema=True)
    row_count = frame.count()
    assert(row_count == 110)
    assert(frame.schema == [("C0", float)])
    data = frame.take(row_count)
    assert(len(data) == frame.count())
    assert(all(isinstance(row[0], float) for row in data))
    # Test use case where we have more than 100 rows of integers and then strings
    data = [[i] for i in xrange(0,100)] + [["xyz" + str(i)] for i in xrange(0,20)]
    frame = tc.frame.create(data, validate_schema=True)
    values = frame.take(frame.count())
    # The last 20 rows of "xyz" should be None since they can't be parsed to integers
    for item in values[100:len(values)]:
        assert(item == [None])

def test_frame_schema_validation(tc):
    """
    Tests explicit schema validation after a frame is created using  validate_pyrdd_schema, and
    checks the number of bad rows
    """
    # test with all integers - schema validation should pass.
    data = [[i] for i in xrange(0, 100)]
    frame = tc.frame.create(data)
    result = frame.validate_pyrdd_schema(frame.rdd, [("a", int)])
    assert(result.bad_value_count == 0)

    # dataset has a mix of integers and strings - schema validation should fail on the strings.
    data = [[i] for i in xrange(0,100)] + [["xyz" + str(i)] for i in xrange(0,20)]
    frame = tc.frame.create(data)
    result = frame.validate_pyrdd_schema(frame.rdd, [("a", int)])
    assert(result.bad_value_count == 20)

    # dataset has a mix of integers and strings, but schema specifies a string.  validation should pass.
    data = [[i] for i in xrange(0,100)] + [["xyz" + str(i)] for i in xrange(0,20)]
    frame = tc.frame.create(data)
    result = frame.validate_pyrdd_schema(frame.rdd, [("a", str)])
    assert(result.bad_value_count == 0)

def test_frame_upload_raw_list_data(tc):
        """does round trip with list data --> upload to frame --> 'take' back to list and compare"""
        data = [[1, 'one', [1.0, 1.1]], [2, 'two', [2.0, 2.2]], [3, 'three', [3.0, 3.3]]]
        schema = [('n', int), ('s', str), ('v', dtypes.vector(2))]
        frame = tc.frame.create(data, schema)
        taken = frame.take(5)
        assert(len(data) == len(taken))
        for r, row in enumerate(taken):
            assert(len(data[r]) == len(row))
            for c, column in enumerate(row):
                assert(data[r][c] == column)

def test_create_empty_frame(tc):
    """
    Tests creating an empty frame.
    """
    frame = tc.frame.create(None)
    assert(frame.count() == 0)
    assert(len(frame.schema) == 0)
    frame = tc.frame.create([])
    assert(frame.count() == 0)
    assert(len(frame.schema) == 0)

def test_invalid_frame_data_source(tc):
    """
    Tests creating a frame with an invalid data source (not a list of data or RDD)
    """
    # create a valid frame to test as a data source
    frame = tc.frame.create([[1, "a"],[2, "b"]])
    assert(frame.count() == 2)
    assert(frame._is_python)

    try:
        # creating a frame from a python frame should not be allowed
        tc.frame.create(frame)
        raise RuntimeError("Expected an error when trying to create a frame from a python frame.")
    except TypeError as e:
        assert("Invalid data source" in e.message)

    frame._scala
    assert(frame._is_scala)

    try:
        # creating a frame from a scala frame should not be allowed
        tc.frame.create(frame)
        raise RuntimeError("Expected an error when trying to create a frame from a scala frame.")
    except TypeError as e:
        assert("Invalid data source" in e.message)

    try:
        # creating a frame from an integer is not allowed
        tc.frame.create(1)
        raise RuntimeError("Expected an error when trying to create a frame from an int.")
    except TypeError as e:
        assert("Invalid data source" in e.message)

    try:
        # if creating a frame from a list, it should be a 2-dimensional list
        tc.frame.create([[1,2,3], 4, [5,6,7]])
        raise RuntimeError("Expected an error when trying to create with an invalid list.")
    except TypeError as e:
        assert("Invalid data source" in e.message)


