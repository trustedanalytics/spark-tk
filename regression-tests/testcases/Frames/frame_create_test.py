##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2014, 2015 Intel Corporation All Rights Reserved.
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
   Usage:  python2.7 frame_create_test.py
   Test interface functionality of
        frame['col_name']
        frame.column_names
        frame.add_columns
        frame.rename_columns
        frame.drop_columns
        frame.name
"""
__author__ = "WDW"
__credits__ = ["Grayson Churchel", "Prune Wickart"]
__version__ = "03.11.2014.001"


# Functionality tested:
#  CsvFile(source, schema, delimiter, skip)
#    file name
#        local & absolute file paths
#        empty file (0 bytes)
#        empty file name
#    delimiter
#        default (comma)
#        explicit comma
#        other characters
#        white space
#        None
#        null ""
#    schema
#        trivial
#        use all data types
#        dup column name (same type)
#        dup column name (diff type)
#        empty schema
#        None
#        schema doesn't match file contents
#        invalid list
#        invalid argument type
#    skip
#        default (0)
#        explicit 0
#        explicit 1
#        many (over half the file)
#        > file length
#        negative
#    import
#        column names & types appear in header
#        bad lines are filtered out
#        ... and appear in error_frame
#  ia.Frame(source)
#    [note: most variations are covered in the above schema tests]
#        Create frames on above schemas
#        None (null source)
#    name
#        default
#        None
#        null string
#        normal name
#        special characters
#        unicode
#   frame.name attribute
#        name is as expected
#        assign new name
#        assign empty name
#        assign invalid name
#
# This test case replaces
#   harnesses
#     <none>
#   TUD
#     delimCheck*
#     ImportEmpty
#     WhitespacesImport
#     TypesTest
#     TypesTest2

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import requests.exceptions
import unittest

import trustedanalytics as ia

from qalib import frame_utils
from qalib import common_utils as cu
from qalib import atk_test
from qalib import config as cf


class FrameCreateTest(atk_test.ATKTestCase):

    def setUp(self):
        """Build frames to be exercised and establish known baselines"""
        # used for testing row counts
        super(FrameCreateTest, self).setUp()

        self.known_row_count = 32

        self.dataset = "int_str_int.csv"
        self.schema = [("num1", ia.int32), ("letter", str), ("num2", ia.int32)]
        self.std_frame = frame_utils.build_frame(
            self.dataset, self.schema, self.prefix)

        # create standard, defunct, and empty frames"
        # this is a lot of work to be doing every single test when
        # it is only needed to be run 1 time
        self.dataset_int_str = cf.data_location+"/int_str_int.csv"
        self.dataset_passwd = cf.data_location+"/passwd.csv"
        self.dataset_addcol = cf.data_location+"/AddCol01.csv"
        self.dataset_typestest = cf.data_location+"/typesTest3.csv"
        self.dataset_dos = cf.data_location+"/dos-types1.csv"
        self.dataset_delimC = cf.data_location+"/delimTest1.csv"
        self.dataset_delimT = cf.data_location+"/delimTest1.tsv"

        self.empty_frame = ia.Frame()

        self.passwd_schema = [("userid", str),
                              ("password", str),
                              ("usrNum", ia.int32),
                              ("grpNum", ia.int32),
                              ("userName", str),
                              ("home", str),
                              ("shell", str)]

        self.white_schema = [("col_A", ia.int32),
                             ("col_B", ia.int64),
                             ("col_3", ia.float32),
                             ("Double", ia.float64),
                             ("Text", str)]

        self.delimT_schema = [("col_A", ia.int32),
                              ("col_B", ia.int64),
                              ("Float", ia.float32),
                              ("Double", ia.float64),
                              ("Text", str)]

        self.addcol_schema = [("col_A", ia.int32),
                              ("col_B", ia.int32),
                              ("col_C", ia.int32)]

        self.typestest_schema = [("col_A", ia.int32),
                                 ("col_B", ia.int64),
                                 ("col_C", ia.float32),
                                 ("Double", ia.float64),
                                 ("Text", str)]

        self.dos_schema = [("col_A", ia.int32),
                           ("col_B", ia.int64),
                           ("col_3", ia.float32),
                           ("Double", ia.float64),
                           ("Text", str)]

    def test_frame_invalid_column(self):
        """Tests retrieving an invalid column errors."""
        # TODO: write this test case
        pass

    def test_frame_status(self):
        """ Frame status check """
        self.assertEqual(self.std_frame.status, "ACTIVE")
        ia.drop_frames(self.std_frame)
        self.assertEqual(self.std_frame.status, "DROPPED")

    def test_frame_last_read(self):
        """ Frame status check """
        old_read = self.std_frame.last_read_date
        print self.std_frame.inspect()
        new_print = self.std_frame.last_read_date
        self.assertNotEqual(old_read, new_print)

    def test_frame_create_row_count(self):
        """ Trivial Frame creation. """

        self.assertEqual(self.empty_frame.row_count, 0,
                         "Empty frame should have 0 row_count")
        self.assertEqual(len(self.empty_frame.take(0)),
                         0, "Empty frame should have no columns")
        self.assertEqual(self.std_frame.row_count, 3,
                         "basic frame should have 3 row_count")

    def test_name_change(self):
        """ Trivial Frame creation with name variations """

        frame_name = cu.get_a_name("Regular_Frame_Name")
        alter_name = cu.get_a_name("Alternate_Name")
        frame = ia.Frame(name=frame_name)

        # Given name is retrievable
        self.assertEqual(frame_name, frame.name)

        # Name can be changed
        frame.name = alter_name
        self.assertIn(alter_name, ia.get_frame_names())
        self.assertNotIn(frame_name, ia.get_frame_names())

        # get_frame retrieves proper frame
        handle = ia.get_frame(frame.name)
        self.assertEqual(handle.name, frame.name)
        self.assertEqual(handle, frame)
        ia.drop_frames(frame)

    def test_name_error(self):
        """ Trivial Frame name errors """

        # Given name is empty
        self.assertRaises(RuntimeError, ia.Frame, name="")

        # Given name is illegal
        self.assertRaises(RuntimeError, ia.Frame, name="! *2")

    def test_schema_empty_name_error(self):
        """ CsvFile Creation fails with empty name."""
        self.assertRaises(ValueError, ia.CsvFile, "", self.schema)

    def test_schema_duplicate_names_diff_type(self):
        """CsvFile creation fails with duplicate names, different type."""
        bad_schema = [("userid", str), ("password", str),
                      # same name, diff type
                      ("userid", ia.int32),
                      ("grpNum", ia.int32), ("userName", str), ("home", str),
                      ("shell", str)]
        passwd_desc = ia.CsvFile(self.dataset_passwd,
                                 bad_schema, delimiter=':')
        self.assertRaises(
            ia.rest.command.CommandServerError, ia.Frame, passwd_desc)

    def test_schema_duplicate_names_same_type(self):
        """CsvFile creation fails with duplicate names, same type."""
        bad_schema = [("userid", str), ("password", str), ("usrNum", ia.int32),
                      ("grpNum", ia.int32), ("userName", str),
                      ("userid", str),      # same name and type
                      ("shell", str)]
        passwd_desc = ia.CsvFile(
            self.dataset_passwd, bad_schema, delimiter=':')
        self.assertRaises(
            ia.rest.command.CommandServerError, ia.Frame, passwd_desc)

    def test_frame_name(self):
        """ Trivial Frame creation with naming variations """

        csv = ia.CsvFile(self.dataset_delimC,
                         self.delimT_schema,
                         delimiter='~')    # Use ~ as a field delimiter

        # Do some things. This currently doesn't do anything but check that
        # exceptions aren't thrown
        frame = ia.Frame()
        frame_name = frame.name
        print frame_name
        self.assertIsNone(frame_name)
        ia.drop_frames(frame)

        frame = ia.Frame(csv, name=None)
        frame_name = frame.name
        self.assertIsNone(frame_name)
        ia.drop_frames(frame)

        frame_name = cu.get_a_name("Regular_Frame_Name")
        frame = ia.Frame(csv, name=frame_name)
        self.assertIn(frame_name, ia.get_frame_names())
        ia.drop_frames(frame)
        self.assertNotIn(frame_name, ia.get_frame_names())

    def test_frame_schema_name_empty(self):
        """Frame creation with empty name fails."""
        csv = ia.CsvFile(self.dataset_delimT,
                         self.delimT_schema,
                         delimiter='~')    # Use ~ as a field delimiter
        self.assertRaises(RuntimeError, ia.Frame, csv, name="")

    def test_frame_name_special_characters(self):
        """Frame creation with special characters in the name fails."""
        csv = ia.CsvFile(self.dataset_delimT,
                         self.delimT_schema,
                         delimiter='~')    # Use ~ as a field delimiter
        self.assertRaises(RuntimeError, ia.Frame, csv,
                          name="Special!@#$%^&*()_+~|}{[]\:;?><,./FrameName")

    def test_schema_invalid_type(self):
        """CsvFile cration with a schema of invalid type fails."""
        bad_schema = -77
        self.assertRaises(TypeError, ia.CsvFile,
                          self.dataset_delimC,
                          bad_schema, delimiter='~')

    def test_schema_invalid_format(self):
        """CsvFile creation fails with a malformed schema."""
        bad_schema = [ia.int32, ia.int64, ia.float32, ia.float64, str]
        self.assertRaises(TypeError, ia.CsvFile,
                          self.dataset_delimC,
                          bad_schema, delimiter='~')


    def test_frame_delim_colon(self):
        """Test building a frame with a colon delimiter."""
        passwd_desc = ia.CsvFile(self.dataset_passwd,
                                 self.passwd_schema,
                                 delimiter=':')  # file not found
        passwd_frame = ia.Frame(passwd_desc)
        self.assertEqual(len(passwd_frame.take(1)[0]),
                         len(self.passwd_schema),
                         "Frame should have 0 row_count")

    def test_frame_delim_tab(self):
        """Test building a frame with a tab delimiter."""
        # print "Create frame with whitespace delimiter"
        csv = ia.CsvFile(self.dataset_delimT,
                         self.white_schema,
                         delimiter='\t')    # Use tab as a field delimiter
        frame = ia.Frame(csv)
        self.assertEqual(len(frame.take(1)[0]),
                         len(self.white_schema),
                         "Frame should have 0 row_count")

    def test_delimiter_none(self):
        """Test a delimiter of None errors."""
        self.assertRaises(ValueError,
                          ia.CsvFile, self.dataset_passwd,
                          self.passwd_schema, delimiter=None)

    def test_delimiter_empty(self):
        """Test an empty delimiter errors."""
        self.assertRaises(ValueError, ia.CsvFile,
                          self.dataset_passwd,
                          self.passwd_schema, delimiter="")


    def test_baseline_rows(self):
        """Test the baseline number of row without modification is correct."""
        # Get baseline number of rows.
        # The file originally had 32 rows of data, but make certain.
        print "Create: count rows, use explicit comma delimiter"
        csv = ia.CsvFile(self.dataset_addcol,
                         schema=self.addcol_schema,
                         delimiter=',',
                         skip_header_lines=0)
        frame = ia.Frame(csv)
        baseline_row_count = frame.row_count
        print "baseline rows", baseline_row_count
        self.assertEqual(self.known_row_count, baseline_row_count)

    def test_skip_2_lines(self):
        """Test skip 2 header lines."""
        # Use skip_header_lines to skip 2 lines
        print "Create skipping 2 header lines"
        csv = ia.CsvFile(self.dataset_addcol,
                         schema=self.addcol_schema,
                         skip_header_lines=2)
        frame = ia.Frame(csv)
        self.assertEqual(frame.row_count,
                         self.known_row_count-2,
                         "Frame should have lost 2 rows")

    def test_skip_all_lines(self):
        """Test skipping all lines."""
        print "Create skipping all header lines"
        csv = ia.CsvFile(self.dataset_addcol,
                         schema=self.addcol_schema,
                         skip_header_lines=self.known_row_count)
        frame = ia.Frame(csv)
        print "Frame rows", frame.row_count
        # Note: This has somewhat non-deterministic behavior
        # due to the splitting of files across hdfs servers
        # On a single node this output will be much different from
        # multi node
        self.assertLess(frame.row_count, 32,
                        ("Expected to lose as many rows as possible rows. "
                         "Have %d out of 32" % frame.row_count))

    def test_skip_none(self):
        """Test skipping no header lines."""
        # Define file on server we will import from
        csv = ia.CsvFile(self.dataset_addcol,
                         schema=self.addcol_schema,
                         skip_header_lines=None)
        print "Create with null skip value"
        frame = ia.Frame(csv)
        self.assertEqual(frame.row_count, self.known_row_count)

    def test_skip_negative_lines(self):
        """Test skipping negative lines errors."""
        # Currently raises HTTPError with warning that only
        # 0, 1, 2 are acceptable values.
        # Define file on server we will import from
        csv = ia.CsvFile(self.dataset_addcol,
                         schema=self.addcol_schema,
                         skip_header_lines=-500)

        print "Create with invalid skip value"
        self.assertRaises((requests.exceptions.HTTPError,
                           ia.rest.command.CommandServerError),
                          ia.Frame, csv)


if __name__ == "__main__":
    unittest.main()
