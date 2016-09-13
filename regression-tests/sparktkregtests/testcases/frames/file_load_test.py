""" Tests file_load functionality with bad data"""

import csv
import subprocess
import unittest
from sparktkregtests.lib import sparktk_test


class FileLoadTest(sparktk_test.SparkTKTestCase):

    def test_import_empty(self):
        """Import an empty file."""
        datafile_empty = self.get_file("empty.csv")
        schema_empty = [("col_A", int),
                        ("col_B", int),
                        ("col_C", str),
                        ("col_D", float),
                        ("col_E", float),
                        ("col_F", str)]
        frame = self.context.frame.import_csv(datafile_empty,
                schema=schema_empty)

        self.assertEqual(frame.count(), 0)

        # print "Verify that copies of empty frame are empty."
        null2 = frame.copy()
        self.assertEqual(null2.count(), 0)
        self.assertEqual(frame.schema, null2.schema)

    def test_import_whitespace(self):
        """Build frame with complex quoting and whitespace"""
        file_size = 150

        datafile_white = self.get_file("TextwithSpaces.csv")
        schema_white = [("col_A", str),
                        ("col_B", str),
                        ("col_C", str)]
        white_frame = self.context.frame.import_csv(datafile_white,
                schema=schema_white)

        self.assertEqual(white_frame.count(), file_size)

if __name__ == "__main__":
    unittest.main()

