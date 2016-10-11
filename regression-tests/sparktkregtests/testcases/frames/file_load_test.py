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

