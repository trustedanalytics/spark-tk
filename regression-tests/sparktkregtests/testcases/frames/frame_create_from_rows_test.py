""" Tests import_csv functionality with varying parameters"""

import csv
import subprocess
import unittest
from sparktkregtests.lib import sparktk_test


class FrameCreateTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Build frames to be exercised and establish known baselines"""
        super(FrameCreateTest, self).setUp()
        self.dataset = [["Bob", 30, 8], ["Jim", 45, 9.5], ["Sue", 25, 7], ["George", 15, 6], ["Jennifer", 18, 8.5]]
        self.schema = [("num1", int), ("letter", str), ("num2", int)]
        self.frame = self.context.frame.create(self.frame_data,
                schema=self.schema)

    def test_frame_invalid_column(self):
        """Tests retrieving an invalid column errors."""
        with self.assertRaises(ValueError):
            self.frame.take(100, columns=['not_in'])

    def test_frame_create_row_count(self):
        """ Trivial Frame creation. """
        frame = self.context.frame.create(self.dataset,
                schema=self.schema)
        self.assertEqual(frame.count(), len(self.dataset))
        self.assertEqual(len(frame.take(3).data), 3)
        # test to see if taking more rows than exist still
        # returns only the right number of rows
        self.assertEqual(len(frame.take(10).data), len(self.dataset))

    def test_schema_duplicate_names_diff_type(self):
        """CsvFile creation fails with duplicate names, different type."""
        # double num1's same type
        bad = [("num1", int), ("num1", str), ("num2", int)]
        with self.assertRaisesRegexp(Exception, "more than one char"):
            self.context.frame.create(self.dataset, schema=bad)

    def test_schema_duplicate_names_same_type(self):
        """CsvFile creation fails with duplicate names, same type."""
        # two num1's with same type
        bad = [("num1", int), ("num1", int), ("num2", int)]
        with self.assertRaisesRegexp(Exception, "more than one char"):
            self.context.frame.create(self.dataset, schema=bad)

    def test_schema_invalid_type(self):
        """CsvFile cration with a schema of invalid type fails."""
        bad_schema = -77
        with self.assertRaisesRegexp(Exception, "more than one char"):
            self.context.frame.create(self.dataset, schema=bad_schema)

    def test_schema_invalid_format(self):
        """CsvFile creation fails with a malformed schema."""
        bad_schema = [int, int, float, float, str]
        with self.assertRaisesRegexp(Exception, "more than one char"):
            self.context.frame.create(self.dataset, schema=bad_schema)

    def test_without_schema(self):
        """Test import_csv without a specified schema"""
        frame = self.context.frame.create(self.dataset)
        expected_inferred_schema = [("C0", int), ("C1", str), ("C2", int)]
        self.assertEqual(frame.schema, expected_inferred_schema)

    def test_with_no_specified_or_inferred_schema(self):
        """Test import_csv with inferredschema false and no schema"""
        # should default to creating a schema of all strings
        frame = self.context.frame.create(self.dataset, inferschema=False)
        expected_schema = [("C0", str), ("C1", str), ("C2", str)]
        self.assertEqual(frame.schema, expected_schema)

    def test_with_inferred_schema(self):
        """Test import_csv without a specified schema"""
        frame = self.context.frame.import_csv(self.dataset, inferschema=True)
        expected_inferred_schema = [("C0", int), ("C1", str), ("C2", int)]
        self.assertEqual(frame.schema, expected_inferred_schema)

    def test_with_defined_schema_and_inferred_schema_is_true(self):
        """Test with inferredschema true and also a defined schema"""
        # should default to using the defined schema
        frame = self.context.frame.import_csv(self.dataset,
                inferschema=True, schema=self.schema)
        self.assertEqual(frame.schema, self.schema)

    def test_with_header_no_schema(self):
        """Test without a schema but with a header"""
        # inferedschema should use first line of the csv as col names
        frame = self.context.frame.import_csv(self.dataset, header=True)
        expected_schema = [("1", int), ("a", str), ("2", int)]
        self.assertEqual(frame.schema, expected_schema)


if __name__ == "__main__":
    unittest.main()
