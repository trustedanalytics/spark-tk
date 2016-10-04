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

""" Tests import_csv functionality with varying parameters"""

import unittest
from sparktkregtests.lib import sparktk_test


class FrameCreateTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Build frames to be exercised and establish known baselines"""
        super(FrameCreateTest, self).setUp()
        self.dataset = [["Bob", 30, 8], ["Jim", 45, 9.5], ["Sue", 25, 7], ["George", 15, 6], ["Jennifer", 18, 8.5]]
        self.schema = [("C0", str), ("C1", int), ("C2", float)]
        self.frame = self.context.frame.create(self.dataset,
                                               schema=self.schema)

    def test_frame_invalid_column(self):
        """Tests retrieving an invalid column errors."""
        with self.assertRaisesRegexp(Exception, "Invalid column name"):
            self.frame.take(100, columns=['not_in'])

    def test_frame_create_row_count(self):
        """ Trivial Frame creation. """
        frame = self.context.frame.create(self.dataset,
                                          schema=self.schema)
        self.assertEqual(frame.count(), len(self.dataset))
        self.assertEqual(len(frame.take(3)), 3)
        # test to see if taking more rows than exist still
        # returns only the right number of rows
        self.assertEqual(len(frame.take(10)), len(self.dataset))

    def test_schema_duplicate_names_diff_type(self):
        """CsvFile creation fails with duplicate names, different type."""
        # double num1's same type
        bad = [("col1", str), ("col1", int), ("col2", float)]
        with self.assertRaisesRegexp(Exception, "Invalid schema"):
            self.context.frame.create(self.dataset, schema=bad)

    def test_schema_duplicate_names_same_type(self):
        """CsvFile creation fails with duplicate names, same type."""
        # two num1's with same type
        # note that this should only throw an error because
        # the column names are duplicate, not because the
        # column types are not valid, the column types being invalid
        # should only trigger an exception if validate_schema=True
        bad = [("col1", int), ("col1", int), ("col2", int)]
        with self.assertRaisesRegexp(Exception, "Invalid schema"):
            self.context.frame.create(self.dataset, schema=bad)

    def test_schema_invalid_type(self):
        """CsvFile cration with a schema of invalid type fails."""
        bad_schema = -77
        with self.assertRaisesRegexp(Exception, "Invalid schema"):
            self.context.frame.create(self.dataset, schema=bad_schema)

    def test_schema_invalid_format(self):
        """CsvFile creation fails with a malformed schema."""
        bad_schema = [int, int, float, float, str]
        with self.assertRaisesRegexp(Exception, "Invalid schema"):
            self.context.frame.create(self.dataset, schema=bad_schema)

    def test_without_schema(self):
        """Test import_csv without a specified schema"""
        frame = self.context.frame.create(self.dataset)
        self.assertEqual(frame.schema, self.schema)

    def test_with_validate_schema_no_schema_provided(self):
        """Test import_csv without a specified schema"""
        frame = self.context.frame.create(self.dataset, validate_schema=True)
        self.assertEqual(frame.schema, self.schema)

    def test_with_validate_schema_with_valid_schema(self):
        """Test with validate_schema true and also a valid schema"""
        # should default to using the defined schema
        frame = self.context.frame.create(self.dataset,
                                          validate_schema=True,
                                          schema=self.schema)
        self.assertEqual(frame.schema, self.schema)

    def test_validate_schema_with_invalid_schema_all_columns_same_datatype(self):
        """Test with validate_schema=True and invalid schema, columns same type"""
        invalid_schema = [("col1", int), ("col2", int), ("col3", int)]
        validated_frame = self.context.frame.create(self.dataset,
                                                    validate_schema=True,
                                                    schema=invalid_schema)
        for row in validated_frame.take(validated_frame.count()):
            for item in row:
                if type(item) is not int:
                    self.assertEqual(item, None)

    def test_validate_schema_with_invalid_schema_col_dif_datatypes(self):
        """Test with validate schema true and column datatypes inconsistent"""
        dataset = [(98, 55), (3, 24), ("Bob", 30)]
        schema = [("col1", int), ("col2", int)]
        frame = self.context.frame.create(dataset,
                                              schema=schema,
                                              validate_schema=True)
        for row in frame.take(frame.count()):
            for item in row:
                if type(item) is not int:
                    self.assertEqual(item, None)

    def test_validate_schema_of_strs(self):
        """Test validate schema true with schema of strs"""
        schema = [("C0", str), ("C1", str), ("C2", str)]
        # should not throw an exception
        # if the datatype can be cast to the schema-specified
        # datatype validate schema should just cast it
        # since ints and floats can be cast to string
        # it should not error but should cast all of the data to strings
        frame = self.context.frame.create(self.dataset, schema=schema, validate_schema=True)
        for row in frame.take(frame.count()):
            # the data should all be cast to str by validate_schema=True
            for item in row:
                self.assertEqual(type(item), str)


if __name__ == "__main__":
    unittest.main()
