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

""" Tests unflatten functionality"""

import unittest
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))
from qalib import sparktk_test


class Unflatten(sparktk_test.SparkTKTestCase):

    def test_unflatten_basic(self):
        """ test for unflatten comma-separated rows """
        datafile_unflatten = self.get_file("unflatten_data_no_spaces.csv")
        schema_unflatten = [("user", str),
                            ("day", str),
                            ("time", str),
                            ("reading", int)]

        frame = self.context.frame.import_csv(datafile_unflatten, schema=schema_unflatten)

        datafile_unflatten_sparse = self.get_file("unflatten_data_sparse.csv")
        schema_unflatten_sparse = [("user", int),
                                   ("day", str),
                                   ("time", str),
                                   ("reading", str)]

        frame_sparse = self.context.frame.import_csv(
            datafile_unflatten_sparse, schema=schema_unflatten_sparse)

        frame.unflatten_columns(['user', 'day'])

        unflat_copy = frame.download()
        for index, row in unflat_copy.iterrows():
            self.assertEqual(
                len(str(row['time']).split(',')),
                len(str(row['reading']).split(',')))

        self.assertEqual(frame.row_count, 1 * 5)

        frame_sparse.unflatten_columns(['user', 'day'])
        unflat_sparse_copy = frame_sparse.download()

        for index, row in unflat_sparse_copy.iterrows():
            self.assertEqual(
                len(str(row['time']).split(',')),
                len(str(row['reading']).split(',')))

        self.assertEqual(frame_sparse.row_count, 2 * 100)


if __name__ == "__main__":
    unittest.main()
