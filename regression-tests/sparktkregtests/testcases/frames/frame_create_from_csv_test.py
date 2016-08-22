""" Tests import_csv functionality with varying parameters"""

import csv
import subprocess
import unittest
from sparktkregtests.lib import sparktk_test


class FrameImportCSVTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Build frames to be exercised and establish known baselines"""
        super(FrameImportCSVTest, self).setUp()
        self.dataset = self.get_file("int_str_int.csv")
        self.schema = [("num1", int), ("letter", str), ("num2", int)]
        self.frame = self.context.frame.import_csv(self.dataset,
                schema=self.schema)

    def test_frame_invalid_column(self):
        """Tests retrieving an invalid column errors."""
        with self.assertRaises(ValueError):
            self.frame.take(100, columns=['not_in'])

    def test_frame_create_row_count(self):
        """ Trivial Frame creation. """
        frame = self.context.frame.import_csv(self.dataset,
                schema=self.schema)
        self.assertEqual(frame.row_count, 3)
        self.assertEqual(len(frame.take(3).data), 3)
        # test to see if taking more rows than exist still
        # returns only the right number of rows
        self.assertEqual(len(frame.take(10).data), 3)

    def test_schema_duplicate_names_diff_type(self):
        """CsvFile creation fails with duplicate names, different type."""
        # double num1's same type
        bad = [("num1", int), ("num1", str), ("num2", int)]
        with self.assertRaisesRegexp(Exception, "more than one char"):
            self.context.frame.import_csv(self.dataset, bad)

    def test_schema_duplicate_names_same_type(self):
        """CsvFile creation fails with duplicate names, same type."""
        # two num1's with same type
        bad = [("num1", int), ("num1", int), ("num2", int)]
        with self.assertRaisesRegexp(Exception, "more than one char"):
            self.context.frame.import_csv(self.dataset, bad)

    def test_schema_invalid_type(self):
        """CsvFile cration with a schema of invalid type fails."""
        bad_schema = -77
        with self.assertRaisesRegexp(Exception, "more than one char"):
            self.context.frame.import_csv(self.dataset, bad_schema)

    def test_schema_invalid_format(self):
        """CsvFile creation fails with a malformed schema."""
        bad_schema = [int, int, float, float, str]
        with self.assertRaisesRegexp(Exception, "more than one char"):
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

        # here we are getting the lines from the csv file from hdfs
        file = subprocess.Popen(["hdfs", "dfs", "-cat", str(dataset_passwd)],
                stdout=subprocess.PIPE)
        lines = []
        for line in iter(file.stdout.readline, ''):
            lines.append(line)

        # now we put the csv file into an array for comparison
        reader = csv.reader(lines, delimiter=':')
        csv_list = list(reader)

        # we create the frame from the csv file
        passwd_frame = self.context.frame.import_csv(dataset_passwd,
                schema=passwd_schema, delimiter=':')

        # finally we compare the frame data with what we got from reading
        # the csv file, we check for length and content
        self.assertEqual(len(passwd_frame.take(1).data[0]),
                         len(passwd_schema))
        passwd_frame_rows = passwd_frame.take(passwd_frame.row_count).data
        for (frame_row, array_row) in zip(passwd_frame_rows, csv_list):
            self.assertEqual(str(map(str, frame_row)), str(array_row))

    def test_frame_delim_tab(self):
        """Test building a frame with a tab delimiter."""
        dataset_delimT = self.get_file("delimTest1.tsv")
        white_schema = [("col_A", int),
                        ("col_B", long),
                        ("col_3", float),
                        ("Double", float),
                        ("Text", str)]

        # create our frame and test that it has the right number of columns
        tab_delim_frame = self.context.frame.import_csv(dataset_delimT,
                schema=white_schema, delimiter='\t')
        self.assertEqual(len(tab_delim_frame.take(1).data[0]),
                         len(white_schema))

        # now we get the lines of data from the csv file for comparison
        file = subprocess.Popen(["hdfs", "dfs", "-cat", str(dataset_delimT)],
                stdout=subprocess.PIPE)
        lines = []
        for line in iter(file.stdout.readline, ''):
            lines.append(line)

        # we store the lines in an array
        reader = csv.reader(lines, delimiter='\t')
        csv_list = list(reader)

        # finally we extract the data from the frame and compare it to
        # what we got from reading the csv file directly
        delim_frame_rows = tab_delim_frame.take(tab_delim_frame.row_count).data
        for (frame_row, array_row) in zip(delim_frame_rows, csv_list):
            # we must iterate through the items in each line
            # because they are of different data types
            # and if we compare them just as strings or list it will fail
            # since the float number convert to slightly differnet values
            # depending on how they are read
            for (frame_item, array_item) in zip(frame_row, array_row):
                try:
                    # the values will be slightly different
                    # for the float vars because they are read
                    # differently by our csv reader
                    # so we use almostEqual
                    self.assertAlmostEqual(float(frame_item),
                            float(array_item))
                except:
                    # if they are not floats
                    # then just compare them as strings
                    self.assertEqual(str(frame_item),
                            str(array_item))

    def test_delimiter_none(self):
        """Test a delimiter of None errors."""
        with self.assertRaisesRegexp(Exception, "delimiter"):
            self.context.frame.import_csv(self.dataset,
                    self.schema, delimiter=None)

    def test_delimiter_empty(self):
        """Test an empty delimiter errors."""
        with self.assertRaisesRegexp(Exception, "delimiter"):
            self.context.frame.import_csv(self.dataset,
                    self.schema, delimiter="")

    def test_header(self):
        """Test header = True"""
        frame_with_header = self.context.frame.import_csv(
            self.dataset, schema=self.schema, header=True)
        frame_without_header = self.context.frame.import_csv(self.dataset,
                schema=self.schema, header=False)

        # the frame with the header should have one less row
        # because it should have skipped the first line
        self.assertEqual(len(frame_with_header.take(frame_with_header.row_count).data),
                         len(frame_without_header.take(frame_without_header.row_count).data) - 1) 
        # comparing the content of the frame with header and without
        # they should have the same rows with the only differnce being the
        # frame with the header should not have the first row
        for index in range(0, frame_with_header.row_count):
            self.assertEqual(str(frame_with_header.take(frame_with_header.row_count).data[index]),
                             str(frame_without_header.take(frame_without_header.row_count).data[index + 1]))

    def test_without_schema(self):
        """Test import_csv without a specified schema"""
        frame = self.context.frame.import_csv(self.dataset)
        expected_inferred_schema = [("C0", int), ("C1", str), ("C2", int)]
        self.assertEqual(frame.schema, expected_inferred_schema)

    def test_with_no_specified_or_inferred_schema(self):
        """Test import_csv with inferredschema false and no schema"""
        # should default to creating a schema of all strings
        frame = self.context.frame.import_csv(self.dataset, inferschema=False)
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
