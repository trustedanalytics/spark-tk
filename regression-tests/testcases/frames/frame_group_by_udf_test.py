#############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2014-2016 Intel Corporation All Rights Reserved.
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
""" Test functionality of group_by, including aggregation_arguments """
import unittest
from operator import itemgetter

import trustedanalytics as ta

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from qalib import common_utils as cu
from qalib import atk_test


class GroupByTest(atk_test.ATKTestCase):

    group_by_udf_data = [[1, "alpha", 3.0, "small", 1, 3.0, 9],
                         [1, "bravo", 5.0, "medium", 1, 4.0, 9],
                         [1, "alpha", 5.0, "large", 1, 8.0, 8],
                         [2, "bravo", 8.0, "large", 1, 5.0, 7],
                         [2, "charlie", 12.0, "medium", 1, 6.0, 6],
                         [2, "bravo", 7.0, "small", 1, 8.0, 5],
                         [2, "bravo", 12.0, "large",  1, 6.0, 4]]

    def setUp(self):
        """Initialize test data at beginning of test"""
        super(GroupByTest, self).setUp()

        schema_colors = [("Int32_0_15", ta.int32),
                         ("Int32_0_31", ta.int32),
                         ("colors", unicode),
                         ("Int64_0_15", ta.int64),
                         ("Int64_0_31", ta.int64),
                         ("Float32_0_15", ta.float32),
                         ("Float32_0_31", ta.float32),
                         ("Float64_0_15", ta.float64),
                         ("Float64_0_31", ta.float64)]

        dataset = cu.get_file("colors_32_9cols_128rows.csv")

        self.frame = ta.Frame(ta.CsvFile(dataset, schema_colors))

    def test_udf_agg_types(self):
        """Run documentation example"""

        expected_take = [[1, u'bravo', 5, 5.0, 1, 1.0, u'r'],
                         [2, u'charlie', 12, 12.0, 2, 0.5, u'a'],
                         [1, u'alpha', 8, 15.0, 2, 1.0, u'll'],
                         [2, u'bravo', 27, 672.0, 6, 0.125, u'aaa']]

        schema = [("a", int), ("b", str), ("c", ta.float64),
                  ("d", str), ("e", int), ("f", ta.float64), ("g", int)]
        frame = ta.Frame(ta.UploadRows(self.group_by_udf_data, schema))

        def custom_agg(acc, row):
            acc.c_sum += row.c
            acc.c_prod *= row.c
            acc.a_sum += row.a
            acc.a_div /= row.a
            acc.b_name += row.b[row.a]

        sum_prod_frame = frame.group_by(
            ['a', 'b'],
            ta.agg.udf(aggregator=custom_agg,
                       output_schema=[('c_sum', ta.int64),
                                      ('c_prod', ta.float64),
                                      ('a_sum', ta.int32),
                                      ('a_div', ta.float32),
                                      ('b_name', str)],
                       init_values=[0, 1, 0, 1, ""]))

        sum_prod_frame.sort('c_prod')

        actual_take = sum_prod_frame.take(sum_prod_frame.row_count)
        actual_take.sort(key=itemgetter(3))

        self.assertEquals(actual_take, expected_take)

    def test_udf_agg_bad_initializer(self):
        """Supply initialization value of wrong type"""
        schema = [("a", int), ("b", str), ("c", ta.float64),
                  ("d", str), ("e", int), ("f", ta.float64), ("g", int)]
        frame = ta.Frame(ta.UploadRows(self.group_by_udf_data, schema))

        def custom_agg(acc, row):
            # col 'c' running sum and product
            acc.c_sum = acc.c_sum + row.c
            acc.c_prod = acc.c_prod * row.c

        with self.assertRaises(ValueError):
            frame.group_by(['a', 'b'],
                           ta.agg.udf(aggregator=custom_agg,
                                      output_schema=[('c_sum', ta.float64),
                                                     ('c_prod', ta.float64)],
                                      init_values=[0, ""]))

    def test_udf_agg_too_few_initialiers(self):
        """Supply too few initialization values"""
        schema = [("a", int), ("b", str), ("c", ta.float64),
                  ("d", str), ("e", int), ("f", ta.float64), ("g", int)]
        frame = ta.Frame(ta.UploadRows(self.group_by_udf_data, schema))

        def custom_agg(acc, row):
            # col 'c' running sum and product
            acc.c_sum = acc.c_sum + row.c
            acc.c_prod = acc.c_prod * row.c

        with self.assertRaises(ValueError):
            frame.group_by(
                ['a', 'b'],
                ta.agg.udf(aggregator=custom_agg,
                           output_schema=[('c_sum', ta.float64),
                                          ('c_prod', ta.float64)],
                           init_values=[0]))

    def test_udf_agg_no_column(self):
        """Add no column: expect error"""
        schema = [("a", int), ("b", str), ("c", ta.float64),
                  ("d", str), ("e", int), ("f", ta.float64), ("g", int)]
        frame = ta.Frame(ta.UploadRows(self.group_by_udf_data, schema))

        def agg_0_col():
            pass

        with self.assertRaises(ValueError):
            frame.group_by(
                ['a', 'b'],
                ta.agg.udf(aggregator=agg_0_col,
                           output_schema=[], init_values=[]))


if __name__ == "__main__":
    unittest.main()
