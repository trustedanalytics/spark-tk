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

"""Tests frame.inspect() """

import unittest
import sys
import os
from sparktkregtests.lib import sparktk_test


class FrameInspectTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Build test frame"""
        super(FrameInspectTest, self).setUp()

        dataset = self.get_file("movie_user_5ratings.csv")
        schema = [("src", int),
                  ("vertex_type", str),
                  ("dest", int),
                  ("weight", int),
                  ("edge_type", str)]

        self.frame = self.context.frame.import_csv(
            dataset, schema=schema)

    def test_frame_inspect_0_offset(self):
        """Test offset of 0 does nothing"""
        inspect = self.frame.inspect(n=5, offset=0)
        self.assertEqual(len(inspect.rows), 5)

    def test_frame_inspect_offset_large(self):
        """Test offset of a large value"""
        inspect = self.frame.inspect(n=5, offset=1000)
        self.assertEqual(len(inspect.rows), 5)

    def test_frame_inspect_offset_overflow(self):
        """Test inspecting more lines than in frrame from offset truncates"""
        inspect = self.frame.inspect(n=10, offset=self.frame.count()-3)
        self.assertEqual(len(inspect.rows), 3)

    def test_frame_inspect_0_count(self):
        """Test inspecting 0 rows returns nothing"""
        inspect = self.frame.inspect(n=0)
        self.assertEqual(len(inspect.rows), 0)

    def test_frame_inspect_n(self):
        """Test requesting n rows returns n rows"""
        inspect = self.frame.inspect(n=1)
        self.assertEqual(len(inspect.rows), 1)

    def test_frame_inspect_default(self):
        """Test the default number of rows is 10"""
        inspect = self.frame.inspect()
        self.assertEqual(len(inspect.rows), 10)

    def test_frame_inspect_all(self):
        """Test inspecting entire frame returns entire frame"""
        inspect = self.frame.inspect(n=self.frame.count())
        self.assertEqual(len(inspect.rows), self.frame.count())

    def test_frame_inspect_count_overflow(self):
        """Test inspecting more than entire frame returns the entire frame"""
        row_count = self.frame.count()
        inspect = self.frame.inspect(n=row_count*10)
        self.assertEqual(len(inspect.rows), row_count)

        #compare 'inspect' with the actual entire frame RowInspection object
        self.assertEqual(str(inspect), 
                         str(self.frame.inspect(n=row_count)))

    def test_negative_offset(self):
        """Test a negative offset errors"""
        with self.assertRaisesRegexp(ValueError, "Expected non-negative integer"):
            self.frame.inspect(n=5, offset=-1)

    def test_negative_count(self):
        """Test taking a negative number of rows errors"""
        with self.assertRaises(ValueError):
            self.frame.inspect(n=-1)

    def test_float_count(self):
        """Test float for count errors"""
        with self.assertRaisesRegexp(TypeError, "Expected type <type 'int'>"):
            self.frame.inspect(n=1.5)

    def test_float_offset(self):
        """Test float for offset errors"""
        with self.assertRaises(TypeError):
            self.frame.inspect(n=1, offset=1.5)

    def test_take_no_columns(self):
        """Test taking an empty list of columns gets an empty list"""
        self.assertEqual([], self.frame.take(n=10, columns=[]))

    def test_take_invalid_column(self):
        """Test taking a column that doesn't exist errors"""
        with self.assertRaisesRegexp(
                ValueError, "Invalid column name .* provided"):
            self.frame.take(n=10, columns=["no_such_col", "weight"])


if __name__ == "__main__":
    unittest.main()
