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

""" Test UDF implementation."""
import unittest
from sparktkregtests.lib import sparktk_test
import udf_remote_utils_direct, udf_remote_utils_indirect, udf_remote_utils_select


class UDFTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        super(UDFTest, self).setUp()
        data_basic = self.get_file("int_str_int.csv")
        schema_basic = [("num1", int),
                        ("letter", str),
                        ("num2", int)]
        self.frame = self.context.frame.import_csv(data_basic,
                                                   schema=schema_basic)

    def test_udf_basic_module_install(self):
        """First test case from UDF testing"""
        # add columns using udf utils
        self.frame.add_columns(
            lambda row:
            udf_remote_utils_direct.length(row.letter) + row.num2,
            ('new_column', int))
        self.frame.add_columns(
            udf_remote_utils_direct.row_build, ('other_column', int))

        # get frame data for the entire frame and for the middle col
        frame_take = self.frame.take(self.frame.count())
        letter_col_take = self.frame.take(self.frame.count(),
                                          columns=['letter'])

        # extract just the letter column into an array of strings
        letter = [x[0].encode("ascii", "ignore") for x in letter_col_take]

        # get the index for the row containing a and c and verify
        a_row = letter.index('a')
        c_row = letter.index('c')
        self.assertEqual(frame_take[c_row][-1], len(frame_take[c_row][1]))
        self.assertEqual(frame_take[a_row][3], frame_take[a_row][2]+1)

    def test_udf_indirect_std(self):
        """test indirect udf usage"""
        # add columns using udf utils
        self.frame.add_columns(
            udf_remote_utils_indirect.distance, ('other_column', float))

        # get the data for the entire frame and just the letter col
        frame_take = self.frame.take(self.frame.count())
        letter_col_take = self.frame.take(self.frame.count(),
                                          columns=['letter'])

        # extract just the letter column into an array of strs
        letter = [x[0].encode("ascii", "ignore") for x in letter_col_take]

        # get the index of the col containing a and c and verify
        a_row = letter.index('a')
        c_row = letter.index('c')
        self.assertAlmostEqual(
            frame_take[c_row][-1], frame_take[c_row][0] * 2.2360679775)
        self.assertAlmostEqual(
            frame_take[a_row][-1], frame_take[a_row][0] * 2.2360679775)

    def test_udf_indirect_delayed(self):
        """Call a remote function."""
        # logic is similar to above two tests
        udf_remote_utils_select.add_select_col(self.frame)

        frame_take = self.frame.take(self.frame.count())
        letter_col_take = self.frame.take(self.frame.count(), columns=['letter'])

        letter = [x[0].encode("ascii", "ignore") for x in letter_col_take]

        a_row = letter.index('a')
        b_row = letter.index('b')
        self.assertEqual(frame_take[a_row][-1], frame_take[a_row][0])
        self.assertEqual(frame_take[b_row][-1], frame_take[b_row][2])

    def test_udf_indirect_missing(self):
        """Use a function that is missing with udf remote"""
        with self.assertRaisesRegexp(Exception, "column name must be alpha-numeric"):
            self.frame.add_columns(lambda row: udf_remote_utils_indirect.not_exist(row.letter) + row.num2,
                                  ("new column", int))
            self.frame.count()


if __name__ == "__main__":
    unittest.main()
