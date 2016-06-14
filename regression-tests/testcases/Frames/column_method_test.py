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
   Usage:  python2.7 column_method_test.py
   Test interface functionality of
        frame['col_name']
        frame.column_names
        frame.add_columns
        frame.rename_columns
        frame.drop_columns
"""
__author__ = "WDW"
__credits__ = ["Abolfazl Shahbazi", "Grayson Churchel", "Prune Wickart"]
__version__ = "2015.03.06"


# Functionality tested:
#   frame['col_name']
#     fetch left, right, and interior columns
#     fetch column with empty name
#     fetch column with unicode in name
#     error: fetch column from dropped frame
#     error: fetch column non-existent name
#   frame.column_names()
#     original column names
#     names for 1 column
#     names for 2 columns
#     names for many columns
#   frame.add_columns()
#     add one column
#     add computed column (lambda)
#     add multiple columns
#     add column with existing name
#     add to dropped frame
#     add with null schema
#     add with empty schema
#     columns_accessed:
#       one col
#       multiple columns
#       multiple columns, out of order
#       missing column
#       extra column (not used)
#       non-existent column
#   frame.rename_columns()
#     rename multiple columns
#     rename one column
#     rename left, right, and interior columns
#     rename to empty string
#     swap column names
#     rename to current name
#     error: rename two columns to same name
#   frame.drop_columns()
#     drop column twice in list
#     drop left, right, and interior columns
#     drop one column / multiple columns
#
# This test case replaces
#   harnesses
#     add_columns
#   TUD
#     AB_test
#     AddCol01
#     AddCOl02
#     ColRenameTest
#     DropCols

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import requests
import unittest

import numpy
import trustedanalytics as ia

from qalib import frame_utils
from qalib import atk_test

dummy_int_val = -77     # placeholder data value for added column
dummy_col_count = 1000  # length of dummy list for column add


# This method is to test different sources of functions
# i.e. global
def global_dummy_val_list(row):
    """
    Return a trivial list of data
    :param row: not_used required by add_columns
    :return: list of length dummy_col_count, all values dummy_int_val
    """
    return [dummy_int_val for _ in range(0, dummy_col_count)]


class ColumnMethodTest(atk_test.ATKTestCase):

    @staticmethod
    def static_dummy_val_list(row):
        """
        Return a trivial list of data
        :param row: not_used required by add_columns
        :return: list of length dummy_col_count, all values dummy_int_val
        """
        return [dummy_int_val for _ in range(0, dummy_col_count)]

    def setUp(self):
        """Build the frame to be exercised"""
        super(ColumnMethodTest, self).setUp()
        dataset = "int_str_int.csv"
        schema = [("a", ia.int32), ("letter", str), ("b", ia.int32)]

        self.frame = frame_utils.build_frame(dataset, schema, self.prefix)

        self.fail_schema = [("col_A", ia.int32),
                            ("col_B", ia.int32),
                            ("col_C", ia.int32)]

    def test_frame_getitem_column(self):
        """Fetch individual and multiple columns"""
        col = self.frame['a']
        self.assertEqual(col.name, "a")
        self.assertEqual(col.data_type, numpy.int32)

        col = self.frame['b']
        self.assertEqual(col.name, "b")
        self.assertEqual(col.data_type, numpy.int32)

        col = self.frame['letter']
        self.assertEqual(col.name, "letter")
        self.assertEqual(col.data_type, unicode)

    def test_no_such_column_name_index(self):
        """Fetching an non-existent column errors"""
        with self.assertRaises(KeyError):
            print self.frame['no-such-name']

    def test_no_such_column_name(self):
        """Fetching an non-existent column errors"""
        print self.frame.b
        with self.assertRaises(AttributeError):
            print self.frame.no_such_name

    def test_column_names(self):
        """all original columns"""
        header = self.frame.column_names
        self.assertEqual(header, ['a', 'letter', 'b'])

    def test_column_names_drop(self):
        """Exercise subsets of 1 and 2 columns"""
        self.frame.drop_columns('letter')
        header = self.frame.column_names
        self.assertEqual(header, ['a', 'b'])

    def test_column_names_drop_multiple(self):
        """Drop multiple columns"""
        self.frame.drop_columns(['b', 'letter'])
        header = self.frame.column_names
        self.assertEqual(header, ['a'])

    def test_static_add_col_names(self):
        """Tests adding a column name"""
        old_header = self.frame.column_names
        new_col_schema = [("col_" + str(n), ia.int32)
                          for n in range(0, dummy_col_count)]
        self.frame.add_columns(ColumnMethodTest.static_dummy_val_list,
                               new_col_schema)
        expected_header = old_header + \
            [col_schema[0] for col_schema in new_col_schema]
        self.assertEqual(self.frame.column_names[0:20], expected_header[0:20])

    @unittest.skip("DPAT-341")
    def test_add_col_names(self):
        """Tests adding a column name"""
        old_header = self.frame.column_names
        new_col_schema = [("col_" + str(n), ia.int32)
                          for n in range(0, dummy_col_count)]
        self.frame.add_columns(global_dummy_val_list, new_col_schema)
        expected_header = old_header + \
            [col_schema[0] for col_schema in new_col_schema]
        self.assertEqual(self.frame.column_names[0:20], expected_header[0:20])

    def test_add_columns(self):
        """
        Test add 1 column.
        Test add multiple columns.
        """
        col_count = len((self.frame.take(1))[0])
        self.frame.add_columns(lambda row: row.a*row.b,
                               ('a_times_b', ia.int32))
        self.assertIn('a_times_b', self.frame.column_names)
        self.assertEqual(col_count+1, len((self.frame.take(1))[0]),
                         "add_columns failed to add exactly one column.")
        self.frame.drop_columns('a_times_b')

        self.frame.add_columns(
            lambda row: [row.a * row.b, row.a + row.b],
            [("a_times_b", ia.float32), ("a_plus_b", ia.float32)])
        self.assertIn('a_times_b', self.frame.column_names)
        self.assertIn('a_plus_b', self.frame.column_names)
        self.assertEqual(col_count+2, len((self.frame.take(1))[0]),
                         "add_columns failed to add two columns.")

    def test_add_columns_accessed_str(self):
        """Test columns_accessed param, 1 col."""
        col_count = len((self.frame.take(1))[0])
        self.frame.add_columns(lambda row: row.b*row.b,
                               ('a_times_b', ia.int32),
                               'b')
        print self.frame.inspect()
        self.assertIn('a_times_b', self.frame.column_names)
        self.assertEqual(col_count+1, len((self.frame.take(1))[0]),
                         "add_columns failed to add exactly one column.")

        self.clean_up = True

    def test_add_columns_accessed_list1(self):
        """Test columns_accessed param, 1 col."""
        col_count = len((self.frame.take(1))[0])
        self.frame.add_columns(lambda row: row.b*row.b,
                               ('a_times_b', ia.int32),
                               ['b'])
        print self.frame.inspect()
        self.assertIn('a_times_b', self.frame.column_names)
        self.assertEqual(col_count+1, len((self.frame.take(1))[0]),
                         "add_columns failed to add exactly one column.")

        self.clean_up = True

    def test_add_columns_accessed_extra(self):
        """Test columns_accessed param; specify an unused column."""
        col_count = len((self.frame.take(1))[0])
        self.frame.add_columns(lambda row: row.b*row.b,
                               ('a_times_b', ia.int32),
                               ['a', 'b'])
        print self.frame.inspect()
        self.assertIn('a_times_b', self.frame.column_names)
        self.assertEqual(col_count+1, len((self.frame.take(1))[0]),
                         "add_columns failed to add exactly one column.")

        self.clean_up = True

    def test_add_columns_accessed_list_all(self):
        """Test columns_accessed param, 1 col."""
        col_count = len((self.frame.take(1))[0])
        self.frame.add_columns(
            lambda row: float(ord(row.letter)) / (row.a*row.b),
            ('a_times_b', ia.float32),
            ['a', 'letter', 'b'])
        print self.frame.inspect()
        self.assertIn('a_times_b', self.frame.column_names)
        self.assertEqual(col_count+1, len((self.frame.take(1))[0]),
                         "add_columns failed to add exactly one column.")

        self.clean_up = True

    def test_add_columns_accessed_list_ooo(self):
        """Test columns_accessed param, all cols out of order."""
        col_count = len((self.frame.take(1))[0])
        self.frame.add_columns(
            lambda row: float(ord(row.letter)) / (row.a*row.b),
            ('poutine', ia.float32),
            ['b', 'a', 'letter'])
        print self.frame.inspect()
        self.assertIn('poutine', self.frame.column_names)
        self.assertEqual(col_count+1, len((self.frame.take(1))[0]),
                         "add_columns failed to add exactly one column.")

        self.clean_up = True

    def test_add_columns_accessed_miss(self):
        """Test columns_accessed param, 1 of 2 needed cols."""
        # TRIB-4362; inadequate error message
        self.assertRaises(ia.rest.command.CommandServerError,
                          self.frame.add_columns,
                          lambda row: row.a*row.b,
                          ('a_times_b', ia.int32),
                          ['a'])

        self.clean_up = True

    def test_add_columns_accessed_nosuch(self):
        """Test columns_accessed param, non-existent col."""
        self.assertRaises(ia.rest.command.CommandServerError,
                          self.frame.add_columns,
                          lambda row: row.b*row.b,
                          ('b_times_b', ia.int32),
                          ['no_such_col'])

        self.clean_up = True

    def test_add_columns_abort(self):
        """
        drop_columns negative testing
          dropped frame
          non-existent column
        """
        self.fail_frame = frame_utils.build_frame(
            "AddCol02.csv", self.fail_schema, self.prefix)
        print self.fail_frame.inspect(10)

        # Divide by 0 exception will abort column add;
        # Schema should be unchanged.
        def bad_divide(row):
            if row.col_C == 0:
                return "Oops!"
            return ia.float32(row.col_A) / ia.float32(row.col_C)

        self.assertRaises(Exception, self.fail_frame.add_columns, bad_divide,
                          schema=["result", ia.float32])

        print "Verify failure did not alter schema"
        after_schema = self.fail_frame.schema
        print self.fail_frame.inspect(10)
        self.assertEqual(self.fail_schema, after_schema)

        # Avoid zero divide; column add should complete
        def bad_divide(row):
            if row.col_C != 0:
                return ia.float64(row.col_A) / ia.float64(row.col_C)
            else:
                return 0

        self.fail_frame.add_columns(bad_divide, schema=["result2", ia.float64])

        print "verify that add column succeeded"
        print self.fail_frame.inspect(10)
        self.assertIn("result2", self.fail_frame.column_names)

    def test_add_columns_add_existing_name(self):
        """Test adding columns iwht existing names errors"""
        print "add column with existing name (error)"
        self.assertRaises((ia.rest.command.CommandServerError,
                           requests.exceptions.HTTPError),
                          self.frame.add_columns,
                          lambda row: dummy_int_val, ('a', ia.int32))

    def test_add_column_with_empty_name(self):
        """Test adding a column with an empty name errors."""
        print "add column with empty name (error)"
        self.assertRaises((ia.rest.command.CommandServerError,
                           requests.exceptions.HTTPError),
                          self.frame.add_columns,
                          lambda row: dummy_int_val, ('', ia.int32))

    def test_add_column_null_schema(self):
        """Test adding a column with a null schema errors."""
        print "add column with null schema"
        self.assertRaises(ValueError, self.frame.add_columns,
                          lambda row: dummy_int_val, None)

    def test_add_column_empty_schema(self):
        """Test adding a column with an empty schema errors."""
        # add column with empty schema
        self.assertRaises(ValueError, self.frame.add_columns,
                          lambda row: dummy_int_val, ())

    def test_add_column_schema_list(self):
        """Test adding a column with a schema containing a list."""
        self.assertRaises(ia.rest.command.CommandServerError,
                          self.frame.add_columns,
                          lambda row: dummy_int_val,
                          schema=[('new_col', ia.int32)])

    def test_rename_columns(self):
        """Test renaming columns works."""
        self.frame.add_columns(lambda row: dummy_int_val,
                               ('product', ia.int32))
        # Expected column names: 'a', 'letter', 'b', 'product'

        # print "rename multiple columns: left and interior"
        col_count = len(self.frame.take(1))
        self.frame.rename_columns({'a': 'firstNumber',
                                   'b': 'secondNumber'})
        self.assertEqual(col_count,
                         len(self.frame.take(1)),
                         "rename_columns changed column count.")
        self.assertNotIn('a',
                         self.frame.column_names,
                         "a column not properly renamed.")
        self.assertNotIn('b',
                         self.frame.column_names,
                         "b column not properly renamed.")
        self.assertIn('firstNumber',
                      self.frame.column_names,
                      "a column not properly renamed.")
        self.assertIn('secondNumber',
                      self.frame.column_names,
                      "b column not properly renamed.")

    def test_unicode_conversion(self):
        """Test renaming with unicode names."""
        col_count = len(self.frame.take(1))
        self.frame.add_columns(lambda row: dummy_int_val,
                               ('product', ia.int32))
        self.frame.rename_columns({'product': u'unicode'})
        self.assertEqual(col_count,
                         len(self.frame.take(1)),
                         "rename_columns changed column count.")
#        self.assertEqual('product' not in
#                        frame.column_names and u'unicode\u24CF'
#                        in frame.column_names,
#            "product column not properly renamed."
        self.assertNotIn('product',
                         self.frame.column_names,
                         "product column not properly renamed.")
        self.assertIn(u'unicode',
                      self.frame.column_names,
                      "product column not properly renamed.")

    def test_redundant_rename(self):
        """Test renaming with the same name works."""
        # print "TRIB-3893 Redundant rename is rejected"
        # print "Rename to same name"
        col_count = len(self.frame.take(1))
        self.frame.rename_columns({'letter': 'letter'})
        self.assertEqual(col_count,
                         len(self.frame.take(1)),
                         "rename_columns changed column count.")
        self.assertIn('letter',
                      self.frame.column_names,
                      "rename_columns failed superfluous name change.")

    def test_swap_column_names(self):
        """Test swapping column names works."""
        # print "TRIB-3894 Swapping column names is rejected"
        # print "Swap column names"
        col_count = len(self.frame.take(1))
        self.frame.rename_columns({'letter': 'a', 'a': 'letter'})
        self.assertEqual(col_count,
                         len(self.frame.take(1)),
                         "rename_columns changed column count.")
        self.assertEqual(u'letter',
                         self.frame.column_names[0],
                         "rename_columns failed name swap 1.")
        self.assertEqual(u'a',
                         self.frame.column_names[1],
                         "rename_columns failed name swap 2.")

        # TRIB-3892 Giving two columns the same name *works*;
        # TRIB-3895   no recovery
        # print "Rename with duplicate name (error)"
        col_count = len(self.frame.take(1))
        self.assertRaises((ia.rest.command.CommandServerError,
                           requests.exceptions.HTTPError),
                          self.frame.rename_columns,
                          {'letter': 'alpha',
                           'b': 'times',
                           'a': 'alpha'})

    def test_rename_non_existent(self):
        """Test renaming a non-existent column fails."""
        print "Rename with non-existent name (error)"
        self.assertRaises((ia.rest.command.CommandServerError,
                           requests.exceptions.HTTPError),
                          self.frame.rename_columns,
                          {'no-such-name': 'not-a-name'})

    def test_rename_existing_name(self):
        """Test renaming to an existing name errors."""
        print "Rename to existing name (error)"
        self.assertRaises((ia.rest.command.CommandServerError,
                           requests.exceptions.HTTPError),
                          self.frame.rename_columns,
                          {'letter': 'a'})

    def test_rename_with_special_characters(self):
        """Test renaming with special characters errors."""
        print "Rename with special characters (error)"
        self.assertRaises((ia.rest.command.CommandServerError,
                           requests.exceptions.HTTPError),
                          self.frame.rename_columns,
                          {'letter': 'Long ugly !@#$%^&*(?)_+|}{[\\]\|'})
        print "The names of the columns are now:\n%s" % self.frame.column_names

    def test_drop_columns(self):
        """
        drop_columns basic testing
          drop left, right, and interior
          drop single and mulitple columns
          drop column multiple times within one call
          drop empty list
        """
        # Expected column names: 'a', 'letter', 'b', 'unicode'
        self.frame.add_columns(lambda row: dummy_int_val,
                               ('product', ia.int32))

        # print "drop left column"
        col_count = len(self.frame.take(1)[0])
        self.frame.drop_columns(['a'])
        self.assertNotIn('a', self.frame.column_names,
                         "drop_columns failed on drop left column.")
        self.assertEqual(col_count-1, len(self.frame.take(1)[0]),
                         "drop_columns failed on drop left column.")

        # TRIB-4027
        # print "drop name column twice in list"
        #       drop right    column
        #       drop interior column
        col_count = len(self.frame.take(1)[0])
        self.frame.drop_columns(['letter', 'product', 'letter'])
        self.assertNotIn('letter', self.frame.column_names,
                         "drop_columns failed on doubly-referenced column")
        self.assertNotIn('product', self.frame.column_names,
                         "drop_columns failed on doubly-referenced column")
        self.assertEqual(col_count-2, len(self.frame.take(1)[0]),
                         "drop_columns failed to remove exactly two columns.")

        # print "drop 0 columns (empty list)"
    def test_drop_zero_columns(self):
        """Test dropping no columns"""
        col_count = len((self.frame.take(1))[0])
        self.frame.drop_columns([])
        self.assertEqual(col_count, len((self.frame.take(1))[0]),
                         "drop_columns failed to properly drop zero columns.")

    def test_drop_nonexistent_column(self):
        """Test drop non-existent column"""
        print "drop nonexistent column (error)"
        self.assertRaises((ia.rest.command.CommandServerError,
                           requests.exceptions.HTTPError),
                          self.frame.drop_columns, ['no-such-name'])

if __name__ == "__main__":
    unittest.main()
