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

    @unittest.skip("offset not implemented")
    def test_frame_inspect_0_offset(self):
        """Test offset of 0 does nothing"""
        inspect = self.frame.inspect(n=5, offset=0)
        self.assertEqual(len(inspect.rows), 5)

    @unittest.skip("offset not implemented")
    def test_frame_inspect_offset_large(self):
        """Test offset of a large value"""
        inspect = self.frame.inspect(n=5, offset=1000)
        self.assertEqual(len(inspect.rows), 5)

    @unittest.skip("offset not implemented")
    def test_frame_inspect_offset_overflow(self):
        """Test inspecting more lines than in frrame from offset truncates"""
        inspect = self.frame.inspect(n=10, offset=self.frame.row_count-3)
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
        inspect = self.frame.inspect(n=self.frame.row_count)
        self.assertEqual(len(inspect.rows), self.frame.row_count)

    def test_frame_inspect_count_overflow(self):
        """Test inspecting more than entire frame returns the entire frame"""
        inspect = self.frame.inspect(n=self.frame.row_count*10)
        self.assertEqual(len(inspect.rows), self.frame.row_count)

        #compare 'inspect' with the actual entire frame RowInspection object
        self.assertEqual(str(inspect), 
                         str(self.frame.inspect(n=self.frame.row_count)))

    @unittest.skip("offset not implemented")
    def test_negative_offset(self):
        """Test a negative offset errors"""
        with self.assertRaisesRegexp(ValueError, "slice indices must be integers"):
            self.frame.inspect(n=5, offset=-1)

    @unittest.skip("bug:does not raise exception")
    def test_negative_count(self):
        """Test taking a negative number of rows errors"""
        with self.assertRaises(ValueError):
            self.frame.inspect(n=-1)

    def test_float_count(self):
        """Test float for count errors"""
        with self.assertRaisesRegexp(TypeError, "slice indices must be integers"):
            self.frame.inspect(n=1.5)

    @unittest.skip("offset not implemented")
    def test_float_offset(self):
        """Test float for offset errors"""
        with self.assertRaises(RuntimeError):
            self.frame.inspect(n=1, offset=1.5)

    def test_take_no_columns(self):
        """Test taking an empty list of columns errors"""
        with self.assertRaisesRegexp(
                ValueError, "Column list must not be empty"):
            self.frame.take(n=10, columns=[])

    def test_take_invalid_column(self):
        """Test taking a column that doesn't exist errors"""
        with self.assertRaisesRegexp(
                ValueError, "Invalid column name .* provided"):
            self.frame.take(n=10, columns=["no_such_col", "weight"])


if __name__ == "__main__":
    unittest.main()
