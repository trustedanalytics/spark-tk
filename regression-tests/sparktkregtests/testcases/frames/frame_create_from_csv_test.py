""" Tests import_csv functionality with varying parameters"""

import sys
import csv
from numpy.testing import assert_almost_equal
import subprocess
import unittest
from sparktkregtests.lib import sparktk_test
import sparktk


class FrameImportCSVTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Build frames to be exercised and establish known baselines"""
        super(FrameImportCSVTest, self).setUp()
        self.dataset = self.get_file("int_str_int.csv")
        self.schema = [("num1", int), ("letter", str), ("num2", int)]
        self.frame = self.context.frame.import_csv(self.dataset, schema=self.schema)

    def test_frame_invalid_column(self):
        """Tests retrieving an invalid column errors."""
        with self.assertRaises(ValueError):
            self.frame.take(100, columns=['not_in'])

    def test_frame_create_row_count(self):
        """ Trivial Frame creation. """
        frame = self.context.frame.import_csv(self.dataset, schema=self.schema)
        self.assertEqual(frame.row_count, 3)
        self.assertEqual(len(frame.take(3).data), 3)
        # test to see if taking more rows than exist still
        # returns only the right number of rows
        self.assertEqual(len(frame.take(10).data), 3)

    def test_schema_duplicate_names_diff_type(self):
        """CsvFile creation fails with duplicate names, different type."""
        # double num1's same type
        bad = [("num1", int), ("num1", str), ("num2", int)]
        with self.assertRaisesRegexp(Exception, "cannot be more than one character"):
            self.context.frame.import_csv(self.dataset, bad)

    def test_schema_duplicate_names_same_type(self):
        """CsvFile creation fails with duplicate names, same type."""
        # two num1's with same type
        bad = [("num1", int), ("num1", int), ("num2", int)]
        with self.assertRaisesRegexp(Exception, "cannot be more than one character"):
            self.context.frame.import_csv(self.dataset, bad)

    def test_schema_invalid_type(self):
        """CsvFile cration with a schema of invalid type fails."""
        bad_schema = -77
        with self.assertRaisesRegexp(Exception, "cannot be more than one character"):
            self.context.frame.import_csv(self.dataset, bad_schema)

    def test_schema_invalid_format(self):
        """CsvFile creation fails with a malformed schema."""
        bad_schema = [int, int, float, float, str]
        with self.assertRaisesRegexp(Exception, "cannot be more than one character"):
            self.context.frame.import_csv(self.dataset, bad_schema)

    def test_frame_delim_colon(self):
        """Test building a frame with a colon delimiter."""
        dataset_passwd = self.get_file("passwd.csv")
        passwd_schema = [("userid", str),
                         ("password", str),
                         ("usrNum", int),
                         ("grpNum", int),
                         ("userName", str),
                         ("home", str),
                         ("shell", str)]
        file = subprocess.Popen(["hdfs", "dfs", "-cat", str(dataset_passwd)], stdout=subprocess.PIPE)
        lines = []
        for line in iter(file.stdout.readline, ''):
            lines.append(line)
        reader = csv.reader(lines, delimiter=':')
        csv_list = list(reader)
        passwd_frame = self.context.frame.import_csv(dataset_passwd, schema=passwd_schema, delimiter=':')
        self.assertEqual(len(passwd_frame.take(1).data[0]), len(passwd_schema))
        for (frame_row, array_row) in zip(passwd_frame.take(passwd_frame.row_count).data, csv_list):
            self.assertEqual(str(map(str, frame_row)), str(array_row))

    def test_frame_delim_tab(self):
        """Test building a frame with a tab delimiter."""
        dataset_delimT = self.get_file("delimTest1.tsv")
        white_schema = [("col_A", int),
                        ("col_B", long),
                        ("col_3", float),
                        ("Double", float),
                        ("Text", str)]
        frame = self.context.frame.import_csv(dataset_delimT, schema=white_schema, delimiter='\t')
        self.assertEqual(len(frame.take(1).data[0]), len(white_schema))
        file = subprocess.Popen(["hdfs", "dfs", "-cat", str(dataset_delimT)], stdout=subprocess.PIPE)
        lines = []
        for line in iter(file.stdout.readline, ''):
            lines.append(line)
        reader = csv.reader(lines, delimiter='\t')
        csv_list = list(reader)
        tab_delim_frame = self.context.frame.import_csv(dataset_delimT, schema=white_schema, delimiter='\t')
        for (frame_row, array_row) in zip(tab_delim_frame.take(tab_delim_frame.row_count).data, csv_list):
            for (frame_item, array_item) in zip(frame_row, array_row):
                try:   
                    self.assertAlmostEqual(float(frame_item), float(array_item))
                except:
                    self.assertEqual(str(frame_item), str(array_item))

    def test_delimiter_none(self):
        """Test a delimiter of None errors."""
        with self.assertRaisesRegexp(Exception, "delimiter"):
            self.context.frame.import_csv(self.dataset, self.schema, delimiter=None)

    def test_delimiter_empty(self):
        """Test an empty delimiter errors."""
        with self.assertRaisesRegexp(Exception, "delimiter"):
            self.context.frame.import_csv(self.dataset, self.schema, delimiter="")

    def test_header(self):
        """Test the baseline number of row without modification is correct."""
        frame_with_header = self.context.frame.import_csv(
            self.dataset, schema=self.schema, header=True)
        frame_without_header = self.context.frame.import_csv(self.dataset, schema=self.schema, header=False)
        self.assertEqual(len(frame_with_header.take(frame_with_header.row_count).data), len(frame_without_header.take(frame_without_header.row_count).data) - 1) 
        # frame with header = True should have on less row than the one without because the first line is being skipped
        for index in range(0, frame_with_header.row_count):
            self.assertEqual(str(frame_with_header.take(frame_with_header.row_count).data[index]), str(frame_without_header.take(frame_without_header.row_count).data[index + 1]))

    def test_without_schema(self):
        """Test import_csv without a specified schema, check that the inferred schema is correct"""
        frame = self.context.frame.import_csv(self.dataset)
        expected_inferred_schema = [("C0", int), ("C1", str), ("C2", int)] # same as self.schema except with generic column labels
        self.assertEqual(frame.schema, expected_inferred_schema)

    def test_with_no_specified_or_inferred_schema(self):
        """Test import_csv with inferredschema false and no specified schema, should default to creating a schema of all strings"""
        frame = self.context.frame.import_csv(self.dataset, inferschema=False)
        expected_schema = [("C0", str), ("C1", str), ("C2", str)]
        self.assertEqual(frame.schema, expected_schema)

    def test_with_inferred_schema(self):
        """Test import_csv without a specified schema, check that the inferred schema is correct"""
        frame = self.context.frame.import_csv(self.dataset, inferschema=True)
        expected_inferred_schema = [("C0", int), ("C1", str), ("C2", int)] # same as self.schema except with generic column labels
        self.assertEqual(frame.schema, expected_inferred_schema)

    def test_with_defined_schema_and_inferred_schema_is_true(self):
        """Test with inferredschema true and also a defined schema, should default to using the defined schema, should no infer the schema"""
        frame = self.context.frame.import_csv(self.dataset, inferschema=True, schema=self.schema)
        self.assertEqual(frame.schema, self.schema)

    def test_with__header_no_schema(self):
        """Test without a schema but with a header, inferedschema should use the first items of the csv as column names"""
        frame = self.context.frame.import_csv(self.dataset, header=True)
        expected_schema = [("1", int), ("a", str), ("2", int)]
        self.assertEqual(frame.schema, expected_schema)	


if __name__ == "__main__":
    unittest.main()
