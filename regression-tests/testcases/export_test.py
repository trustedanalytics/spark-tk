##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2015 Intel Corporation All Rights Reserved.
#
# The source code contained or described herein and all documents related to
# the source code (Material) are owned by Intel Corporation or its suppliers
# or licensors. Title to the Material remains with Intel Corporation or its
# suppliers and licensors. The Material may contain trade secrets and
# proprietary and confidential information of Intel Corporation and its
# suppliers and licensors, and is protected by worldwide copyright and trade
# secret laws and treaty provisions. No part of the Material may be used,
# copied, reproduced, modified, published, uploaded, posted, transmitted,
# distributed, or disclosed in any way without Intel's prior express written
# permission.
#
# No license under any patent, copyright, trade secret or other intellectual
# property right is granted to or conferred upon you by disclosure or
# delivery of the Materials, either expressly, by implication, inducement,
# estoppel or otherwise. Any license under such intellectual property rights
# must be express and approved by Intel in writing.
##############################################################################
"""
    Usage:  python2.7 export_test.py
    Test frame export
"""
__author__ = 'Prune Wickart'
__credits__ = ["Prune Wickart"]
__version__ = "2015.02.10"
"""
Functionality tested:
  export_to_csv()
    export and re-load a long file (1M lines)
  export_to_json()
    export and re-load a long file (1M lines)
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest
import json

import trustedanalytics as ia

from qalib import frame_utils
from qalib import common_utils
from qalib import atk_test
from qalib import config


class FrameExportTest(atk_test.ATKTestCase):

    def setUp(self):
        super(FrameExportTest, self).setUp()
        self.location = config.export_location

        self.data_sum = "cumulative_seq_v2.csv"
        self.schema_sum = [("sequence", ia.int32),
                           ("col1", ia.int32),
                           ("cumulative_sum", ia.int32),
                           ("percent_sum", ia.float64)]

        # Include files with string data, good and bad.
        self.movie_file = "movie.csv"  # use for data_json_str

        self.data_csv_type = "typesTest5.csv"
        self.schema_type = [("col_A", ia.int32),
                            ("col_B", ia.int64),
                            ("col_C", ia.float32),
                            ("Double", ia.float64),
                            ("Text", str)]

        # No schema here:
        #   a JSON file's schema is encoded in the parsing function.

        # Output files
        self.data_csv = "export_test_csv"
        # self.data_csv_abs = "hdfs://master.kimyale2.cluster.gao/tmp/export_test_csv"
        self.data_csv_abs = "hdfs://master.kimyale2.cluster.gao:8020/tmp/export_test_csv_" + \
                            common_utils.get_a_name(self.prefix)
        self.data_csv_abs = self.data_csv_abs.replace("__", "_")
        self.data_csv_abs = "hdfs://master.kimyale2.cluster.gao:8020/tmp/export_test_csv_07"

        self.data_csv_sep = "export_test_csv_sep"
        self.data_csv_str = "export_test_csv_str"
        self.schema_csv = [("sequence", ia.int32),
                           ("col1", ia.int32)]

        self.data_json = "export_test_json"
        self.data_json_abs = "hdfs://master.kimyale2.cluster.gao:8020/tmp/export_test_json"
        self.data_json_str = "export_test_json_str"
        self.schema_str = [('user', ia.int32),
                           ('vertexType', str),
                           ('movie', ia.int32),
                           ('rating', str),
                           ('splits', str)]

    def test_export_csv(self):
        """ export and re-load a file in CSV format """
        export_location = common_utils.get_a_name(self.data_csv)

        frame_orig = frame_utils.build_frame(
            self.data_sum, self.schema_sum, self.prefix)
        frame_orig.drop_columns(["cumulative_sum", "percent_sum"])

        frame_orig.export_to_csv(export_location)

        # Re-load the exported CSV file.
        frame_csv = frame_utils.build_frame(
            export_location, self.schema_csv,
            location=self.location, prefix=self.prefix, skip_header_lines=1)

        print "ORIGINAL\n", frame_orig.inspect(10)
        print "CSV COPY\n", frame_csv.inspect(10)
        self.assertEqual(frame_orig.row_count, frame_csv.row_count)

        # Check the file for data integrity
        frame_orig.sort("sequence")
        take1 = frame_orig.take(frame_orig.row_count)
        frame_csv.sort("sequence")
        take2 = frame_csv.take(frame_csv.row_count)
        self.assertEqual(take1, take2)

    @unittest.skip("File name not yet hardy")
    def test_export_csv_abs_path(self):
        """ export and re-load a file in CSV format """

        # If the output file exists from a previous aborted run, delete it.
        # self.hdfs_conn.remove(config.fs_root+self.data_csv, recurse=True)
        # print "removed any pre-existing CSV folder", self.data_csv_abs

        frame_orig = ia.get_frame(u'Prune_export_frame')
        # print "CHECK A"
        # frame_orig = frame_utils.build_frame(
        #     self.data_sum, self.schema_sum, self.prefix)
        # print "CHECK B"
        # frame_orig.drop_columns(["cumulative_sum", "percent_sum"])
        # frame_orig.name = "Prune_export_frame"
        print "CHECK C"

        frame_orig.export_to_csv(self.data_csv_abs)
        print "CHECK D"
        print "File name:", self.data_csv_abs

        # Re-load the exported CSV file.
        frame_csv = frame_utils.build_frame(
            self.data_csv_abs, self.schema_csv, location=self.location, prefix=self.prefix)

        # Remove the file as soon as we're finished with it.
        self.hdfs_conn.remove(config.fs_root+self.data_csv, recurse=True)
        print "removed existing CSV folder", self.data_csv

        print "ORIGINAL\n", frame_orig.inspect(10)
        print "CSV COPY\n", frame_csv.inspect(10)
        self.assertEqual(frame_orig.row_count, frame_csv.row_count)

        # Check the file for data integrity
        frame_orig.sort("sequence")
        take1 = frame_orig.take(frame_orig.row_count)
        frame_csv.sort("sequence")
        take2 = frame_csv.take(frame_csv.row_count)
        self.assertEqual(take1, take2)

    def test_export_csv_empty(self):
        """ export an empty frame """
        # Export new frame
        # Export frame with all rows dropped

        frame_empty = ia.Frame()
        frame_empty.export_to_csv(common_utils.get_a_name(self.prefix))

        frame_orig = frame_utils.build_frame(
            self.data_sum, self.schema_sum, self.prefix)

        frame_orig.inspect()
        frame_orig.filter(lambda row: False)
        frame_orig.inspect()
        frame_orig.export_to_csv(common_utils.get_a_name(self.prefix))

    def test_export_csv_sep(self):
        """ export and re-load a file in CSV format """
        export_location = common_utils.get_a_name(self.data_csv_sep)

        frame_orig = frame_utils.build_frame(
            self.data_sum, self.schema_sum, self.prefix)
        frame_orig.drop_columns(["cumulative_sum", "percent_sum"])

        frame_orig.export_to_csv(export_location, '|')

        # Re-load the exported CSV file.
        frame_csv = frame_utils.build_frame(
            export_location, self.schema_csv,
            delimit='|', prefix=self.prefix, location=self.location, skip_header_lines=1)

        print "ORIGINAL\n", frame_orig.inspect(10)
        print "CSV COPY\n", frame_csv.inspect(10)
        self.assertEqual(frame_orig.row_count, frame_csv.row_count)

        # Check the file for data integrity
        frame_orig.sort("sequence")
        take1 = frame_orig.take(frame_orig.row_count)
        frame_csv.sort("sequence")
        take2 = frame_csv.take(frame_csv.row_count)
        self.assertEqual(take1, take2)

    def test_export_csv_str(self):
        """
        Export and re-load a file in CSV format.
        Include strings containing the delimiter.
        """
        export_location = common_utils.get_a_name(self.data_csv_str)

        frame_orig = frame_utils.build_frame(
            self.data_csv_type, self.schema_type, self.prefix)
        frame_orig.inspect()

        # Add a column with the separator char in text strings.
        def sep_string(row):
            return row.Text + ', extra stuff'

        frame_orig.add_columns(sep_string, ("NewText", str))
        print frame_orig.inspect(5)
        frame_orig.drop_columns("Text")
        frame_orig.rename_columns({"NewText": "Text"})
        print frame_orig.inspect(5)

        print self.data_csv_str, "... dump to the file"
        frame_orig.export_to_csv(export_location)

        # Re-load the exported CSV file.
        # Don't use build_frame: it makes assumptions about the file location.
        frame_csv = frame_utils.build_frame(
            export_location, self.schema_type,
            self.prefix, location=self.location, skip_header_lines=1)

        print "ORIGINAL\n", frame_orig.inspect(10)
        print "CSV COPY\n", frame_csv.inspect(10)
        self.assertEqual(frame_orig.row_count, frame_csv.row_count)

        # Check the file for data integrity
        frame_orig.sort(["Text", "Double"])
        take1 = frame_orig.take(frame_orig.row_count)
        frame_csv.sort(["Text", "Double"])
        take2 = frame_csv.take(frame_csv.row_count)
        self.assertEqual(take1, take2)

    def test_export_json(self):
        """ export and re-load a file in JSON format """
        export_location = common_utils.get_a_name(self.data_json)

        frame_orig = frame_utils.build_frame(self.data_sum, self.schema_sum)
        frame_orig.drop_columns(["cumulative_sum", "percent_sum"])

        frame_orig.export_to_json(export_location)

        frame_json = frame_utils.build_frame(
            export_location, file_format="json", location=self.location)

        print "ORIGINAL\n", frame_orig.inspect(10)
        print "JSON COPY\n", frame_json.inspect(10)
        self.assertEqual(frame_orig.row_count, frame_json.row_count)

        # TODO: convert to "mutate" flow when available
        def extract_json(row):
            """
            Return the listed fields from the file.
            Return 'None' for any missing element.
            """
            my_json = json.loads(row[0])
            print "LINE", my_json
            sequence = my_json['sequence']
            col1 = my_json['col1']
            return sequence, col1

        frame_json.add_columns(extract_json, [("sequence", ia.int32),
                                              ("col1", ia.int32)])
        frame_json.drop_columns("data_lines")
        print "JSON COPY PARSED\n", frame_json.inspect(10)

        # Check the file for data integrity
        frame_orig.sort("sequence")
        take1 = frame_orig.take(frame_orig.row_count)
        frame_json.sort("sequence")
        take2 = frame_json.take(frame_json.row_count)
        self.assertEqual(take1, take2)

    @unittest.skip("File name not yet hardy")
    def test_export_json_abs(self):
        """ export and re-load a file in JSON format """

        frame_orig = frame_utils.build_frame(self.data_sum, self.schema_sum)
        frame_orig.drop_columns(["cumulative_sum", "percent_sum"])

        frame_orig.export_to_json(self.data_json_abs)

        frame_json = frame_utils.build_frame(
            self.data_json, file_format="json")

        print "ORIGINAL\n", frame_orig.inspect(10)
        print "JSON COPY\n", frame_json.inspect(10)
        self.assertEqual(frame_orig.row_count, frame_json.row_count)

        # TODO: convert to "mutate" flow when available
        def extract_json(row):
            """
            Return the listed fields from the file.
            Return 'None' for any missing element.
            """
            my_json = json.loads(row[0])
            print "LINE", my_json
            sequence = my_json['sequence']
            col1 = my_json['col1']
            return sequence, col1

        frame_json.add_columns(extract_json, [("sequence", ia.int32),
                                              ("col1", ia.int32)])
        frame_json.drop_columns("data_lines")
        print "JSON COPY PARSED\n", frame_json.inspect(10)

        # Check the file for data integrity
        frame_orig.sort("sequence")
        take1 = frame_orig.take(frame_orig.row_count)
        frame_json.sort("sequence")
        take2 = frame_json.take(frame_json.row_count)
        self.assertEqual(take1, take2)

    def test_export_json_str(self):
        """ Test JSON exportation with string data """
        export_location = common_utils.get_a_name(self.data_json_str)

        print "create big frame"
        frame_orig = frame_utils.build_frame(
            self.movie_file, self.schema_str, skip_header_lines=1)

        frame_orig.export_to_json(export_location)

        frame_json = frame_utils.build_frame(
            export_location, file_format="json",
            prefix=self.prefix, location=self.location)

        print "ORIGINAL\n", frame_orig.inspect(10)
        print "JSON COPY\n", frame_json.inspect(10)
        self.assertEqual(frame_orig.row_count, frame_json.row_count)

        def extract_json(row):
            """
            Return the listed fields from the file.
            Return 'None' for any missing element.
            """
            my_json = json.loads(row[0])
            str1 = my_json['vertexType']
            str2 = my_json['rating']
            movie_id = my_json['movie']
            return str1, str2, movie_id

        # The try-except code may be removed when TRIB-4507 is fixed.
        try:
            frame_json.add_columns(extract_json, [("type", str),
                                                  ("rating", str),
                                                  ("movie", ia.int32)])
        except Exception as e:
            print "https://jira01.devtools.intel.com/browse/TRIB-4507"
            raise e

        frame_json.drop_columns("data_lines")
        print "JSON COPY PARSED\n", frame_json.inspect(10)

        # Check the file for data integrity
        frame_proj = frame_orig.copy(["vertexType", "rating", "movie"])
        print frame_proj.inspect(2)
        print frame_json.inspect(2)
        frame_proj.sort(["rating", "movie"])
        frame_json.sort(["rating", "movie"])
        self.assertTrue(frame_utils.compare_frames(frame_orig, frame_json, [("vertexType", "type"), ("rating","rating"), ("movie", "movie")]))


if __name__ == '__main__':
    unittest.main()
