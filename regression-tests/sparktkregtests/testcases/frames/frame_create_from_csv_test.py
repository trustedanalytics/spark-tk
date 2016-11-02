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

import csv
import unittest
import numpy
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
        with self.assertRaisesRegexp(Exception, "Invalid column name"):
            self.frame.take(100, columns=['not_in'])

    def test_frame_create_row_count(self):
        """ Trivial Frame creation. """
        frame = self.context.frame.import_csv(self.dataset,
                                              schema=self.schema)
        self.assertEqual(frame.count(), 3)
        self.assertEqual(len(frame.take(3)), 3)
        # test to see if taking more rows than exist still
        # returns only the right number of rows
        self.assertEqual(len(frame.take(10)), 3)

    def test_schema_duplicate_names_diff_type(self):
        """CsvFile creation fails with duplicate names, different type."""
        # double num1's same type
        bad = [("num1", int), ("num1", str), ("num2", int)]
        with self.assertRaisesRegexp(Exception, "schema has duplicate column names: \[\'num1\'\]"):
            self.context.frame.import_csv(self.dataset, schema=bad)

    def test_given_schema_is_honored(self):
        schema = [("num1", float), ("letter", str), ("num2", int)]
        frame = self.context.frame.import_csv(self.dataset, schema=schema)
        for row in frame.take(frame.count()):
            self.assertEqual(type(row[0]), float)

    #@unittest.skip("import_csv invalid schema error message not helpful")
    def test_schema_duplicate_names_same_type(self):
        """CsvFile creation fails with duplicate names, same type."""
        # two num1's with same type
        bad = [("num1", int), ("num1", int), ("num2", int)]
        with self.assertRaisesRegexp(Exception, "duplicate column name"):
            self.context.frame.import_csv(self.dataset, schema=bad)

    #@unittest.skip("import_csv invalid schema error message not helpful")
    def test_schema_invalid_type(self):
        """CsvFile cration with a schema of invalid type fails."""
        bad_schema = -77
        with self.assertRaisesRegexp(Exception, "Unsupported type <type \'int\'> for schema parameter"):
            self.context.frame.import_csv(self.dataset, schema=bad_schema)

    #@unittest.skip("import_csv invalid schema error message not helpful")
    def test_schema_invalid_format(self):
        """CsvFile creation fails with a malformed schema."""
        bad_schema = [int, int, float, float, str]
        with self.assertRaisesRegexp(Exception, "schema expected to contain tuples, encountered type <type \'type\'>"):
            self.context.frame.import_csv(self.dataset, schema=bad_schema)

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
        data_file = open(self.get_local_dataset("passwd.csv"))
        lines = data_file.readlines()

        # now we put the csv file into an array for comparison
        reader = csv.reader(lines, delimiter=':')
        csv_list = list(reader)

        # we create the frame from the csv file
        passwd_frame = self.context.frame.import_csv(dataset_passwd,
                                                     schema=passwd_schema,
                                                     delimiter=':')

        # finally we compare the frame data with what we got from reading
        # the csv file, we check for length and content
        self.assertEqual(len(passwd_frame.take(1)[0]),
                         len(passwd_schema))
        passwd_frame_rows = passwd_frame.take(passwd_frame.count())

        for frame_row, csv_row in zip(passwd_frame_rows, csv_list):
            for frame_item, csv_item in zip(frame_row, csv_row):
                if type(frame_item) is float:
                    self.assertAlmostEqual(frame_item, float(csv_item))
                elif type(frame_item) is int:
                    self.assertEqual(frame_item, int(csv_item))
                else:
                    self.assertEqual(str(frame_item), csv_item)

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
                                                        schema=white_schema,
                                                        delimiter='\t')
        self.assertEqual(len(tab_delim_frame.take(1)[0]),
                         len(white_schema))

        # now we get the lines of data from the csv file for comparison
        data_file = open(self.get_local_dataset("delimTest1.tsv"))
        lines = data_file.readlines()

        # we store the lines in an array
        reader = csv.reader(lines, delimiter='\t')
        csv_list = list(reader)

        # finally we extract the data from the frame and compare it to
        # what we got from reading the csv file directly
        delim_frame_data = tab_delim_frame.take(tab_delim_frame.count())

        for (frame_row, csv_line) in zip(delim_frame_data, numpy.array(csv_list)):
            for (frame_item, csv_item) in zip(frame_row, csv_line):
                if type(frame_item) is float:
                    self.assertAlmostEqual(frame_item, float(csv_item))
                elif type(frame_item) is int:
                    self.assertEqual(frame_item, int(csv_item))
                else:
                    self.assertEqual(str(frame_item), csv_item)

    def test_delimiter_none(self):
        """Test a delimiter of None errors."""
        with self.assertRaisesRegexp(Exception, "delimiter"):
            self.context.frame.import_csv(self.dataset,
                                          schema=self.schema,
                                          delimiter=None)

    def test_delimiter_empty(self):
        """Test an empty delimiter errors."""
        with self.assertRaisesRegexp(Exception, "delimiter"):
            self.context.frame.import_csv(self.dataset,
                                          schema=self.schema,
                                          delimiter="")

    def test_header(self):
        """Test header = True"""
        frame_with_header = self.context.frame.import_csv(self.dataset,
                                                          schema=self.schema,
                                                          header=True)
        frame_without_header = self.context.frame.import_csv(self.dataset,
                                                             schema=self.schema,
                                                             header=False)

        # the frame with the header should have one less row
        # because it should have skipped the first line
        self.assertEqual(len(frame_with_header.take(frame_with_header.count())),
                         len(frame_without_header.take(frame_without_header.count())) - 1)
        # comparing the content of the frame with header and without
        # they should have the same rows with the only differnce being the
        # frame with the header should not have the first row
        for index in xrange(0, frame_with_header.count()):
            self.assertEqual(str(frame_with_header.take(frame_with_header.count())[index]),
                             str(frame_without_header.take(frame_without_header.count())[index + 1]))

    def test_without_schema(self):
        """Test import_csv without a specified schema"""
        frame = self.context.frame.import_csv(self.dataset)
        expected_inferred_schema = [("C0", int), ("C1", str), ("C2", int)]
        self.assertEqual(frame.schema, expected_inferred_schema)

    def test_import_csv_raw(self):
        """Test import_csv_raw where all data is brought in as strings """
        # should default to creating a schema of all strings
        frame = self.context.frame.import_csv_raw(self.dataset)
        expected_schema = [("C0", str), ("C1", str), ("C2", str)]
        self.assertEqual(frame.schema, expected_schema)

    def test_with_inferred_schema(self):
        """Test import_csv without a specified schema"""
        frame = self.context.frame.import_csv(self.dataset)
        expected_inferred_schema = [("C0", int), ("C1", str), ("C2", int)]
        self.assertEqual(frame.schema, expected_inferred_schema)

    def test_with_defined_schema_and_inferred_schema_is_true(self):
        """Test with inferredschema true and also a defined schema"""
        # should default to using the defined schema
        frame = self.context.frame.import_csv(self.dataset,
                                              schema=self.schema)
        self.assertEqual(frame.schema, self.schema)

    def test_with_header_no_schema(self):
        """Test without a schema but with a header"""
        # inferedschema should use first line of the csv as col names
        frame = self.context.frame.import_csv(self.dataset, header=True)
        expected_schema = [("1", int), ("a", str), ("2", int)]
        self.assertEqual(frame.schema, expected_schema)


if __name__ == "__main__":
    unittest.main()
