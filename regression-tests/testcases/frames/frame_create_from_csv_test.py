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
""" Test interface functionality of column accessors and modifiers"""

import unittest

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))
from qalib import sparktk_test
import sparktk

class FrameCreateTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Build frames to be exercised and establish known baselines"""
        super(FrameCreateTest, self).setUp()
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

    def test_schema_duplicate_names_diff_type(self):
        """CsvFile creation fails with duplicate names, different type."""
        # double num1's same type
        bad = [("num1", int), ("num1", str), ("num2", int)]
        with self.assertRaises(Exception):
            self.context.frame.import_csv(self.dataset, bad)

    def test_schema_duplicate_names_same_type(self):
        """CsvFile creation fails with duplicate names, same type."""
        # two num1's with same type
        bad = [("num1", int), ("num1", int), ("num2", int)]
        with self.assertRaises(Exception):
            self.context.frame.import_csv(self.dataset, bad)

    def test_schema_invalid_type(self):
        """CsvFile cration with a schema of invalid type fails."""
        bad_schema = -77
        with self.assertRaises(Exception):
            self.context.frame.import_csv(self.dataset, bad_schema)

    def test_schema_invalid_format(self):
        """CsvFile creation fails with a malformed schema."""
        bad_schema = [int, int, float, float, str]
        with self.assertRaises(Exception):
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
        passwd_frame = self.context.frame.import_csv(dataset_passwd, schema=passwd_schema, delimiter=':')
        self.assertEqual(len(passwd_frame.take(1).data[0]), len(passwd_schema))

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

    def test_delimiter_none(self):
        """Test a delimiter of None errors."""
        with self.assertRaises(Exception):
            self.context.frame.import_csv(self.dataset, self.schema, delimiter=None)

    def test_delimiter_empty(self):
        """Test an empty delimiter errors."""
        with self.assertRaises(Exception):
            self.context.frame.import_csv(self.dataset, self.schema, delimiter="")

    def test_header(self):
        """Test the baseline number of row without modification is correct."""
        frame_with_header = self.context.frame.import_csv(
            self.dataset, schema=self.schema, header=True)
        frame_without_header = self.context.frame.import_csv(self.dataset, schema=self.schema, header=False)
        self.assertEqual(len(frame_with_header.take(sys.maxint).data), len(frame_without_header.take(sys.maxint).data) - 1) # frame with header = True should have on less row than the one without because the first line is being skipped
    
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
