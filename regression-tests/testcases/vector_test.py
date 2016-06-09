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
   Usage:  python2.7 vector_test.py
   Test frame mutate features
"""
__author__ = "WDW"
__credits__ = ["Prune Wickart"]
__version__ = "2015.02.23"


# Functionality tested:
#   export_csv/json and subsequent load
#   flatten_column
#   download
#   join
#   drop_duplicates
#   filter
#
#   bin_column
#   column_mode
#   copy
#   count, top_k
#   tally, tally_percent
#   entropy
#
# Not yet working (no '<' operation defined):
#   sort
#   median
#   histogram
#
# TODO:
#   Use non-trivial values for representation
#   Test vectors as multiple columns
#   Test multiple columns as vectors
#   Test vector elements as doubles: col_A[1]
#
# Note: the variations in vector columns are to test diverse aspects
#   of vector comparisons.
# In use, vectors in a given column should be of uniform length.
# We need different values.  We also need vectors with the same values,
#   but in different orders.
# Most of these tests work with a 3-D vector of uniform difference;
#   some increasing, some decreasing.

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest
import json

import trustedanalytics as ia

from qalib import frame_utils
from qalib import common_utils
from qalib import hdfs_utils
from qalib import atk_test
from qalib import config


class VectorTest(atk_test.ATKTestCase):

    def setUp(self):
        """ Build the standard types frame. """
        super(VectorTest, self).setUp()
        self.location = config.export_location
        self.dataset = "oregon-cities.csv"

        self.schema = [('rank', ia.int32),
                       ('city', str),
                       ('population_2013', str),
                       ('pop_2010', str),
                       ('change', str),
                       ('county', str)]

        self.schema_save = [('rank', ia.int32),
                            ('city', str),
                            ('population_2013', str),
                            ('pop_2010', str),
                            ('change', str),
                            ('county', str),
                            ('pop_vec', ia.vector(2)),
                            ('pop_delta', ia.float64)]

        self.frame = frame_utils.build_frame(
            self.dataset, self.schema, self.prefix, delimit='|', skip_header_lines=1)

    def test_vector_export_csv(self):
        """ Export to and load from a CSV file """
        out_str = common_utils.get_a_name('test_vector_csv')

        self.frame.add_columns(lambda row: [
            float(row['pop_2010'].translate({ord(','): None})),
            float(row['population_2013'].translate({ord(','): None}))],
            ("pop_vec", ia.vector(2)))

        self.frame.add_columns(lambda row:
                               100*((row.pop_vec[1]/row.pop_vec[0]) - 1.00),
                               ("comp_change", ia.float64))
        self.frame.export_to_csv(out_str, '|')

        self.frame = frame_utils.build_frame(
            out_str, self.schema_save, delimit="|", location=self.location)

        # Validate recovered vector components
        for row in self.frame.take(self.frame.row_count):
            # Check old and new populations.
            pop_vec = row[6]
            old_pop = float(row[3].translate({ord(','): None}))
            new_pop = float(row[2].translate({ord(','): None}))
            self.assertEqual(pop_vec[0], old_pop)
            self.assertEqual(pop_vec[1], new_pop)

    def test_vector_export_json(self):
        """ Export to and load from a JSON file """
        out_str = common_utils.get_a_name('test_vector_json')

        self.frame.add_columns(lambda row: [
            float(row['pop_2010'].translate({ord(','): None})),
            float(row['population_2013'].translate({ord(','): None}))],
            ("pop_vec", ia.vector(2)))

        self.frame.add_columns(lambda row:
                               100*((row.pop_vec[1]/row.pop_vec[0]) - 1.00),
                               ("comp_change", ia.float64))
        self.frame.export_to_json(out_str)

        frame_json = frame_utils.build_frame(
            out_str, file_format="json", location=self.location)

        self.assertEqual(self.frame.row_count, frame_json.row_count)

        def extract_json(row):
            """
            Return the listed fields from the file.
            Return 'None' for any missing element.
            """
            my_json = json.loads(row[0])
            # print "LINE", my_json
            city = my_json['city']
            pop13 = my_json['population_2013'].translate({ord(','): None})
            pop10 = my_json['pop_2010'].translate({ord(','): None})
            pop_vec = my_json['pop_vec']
            return city, pop13, pop10, pop_vec

        frame_json.add_columns(extract_json, [("city", str),
                                              ("pop13", ia.int32),
                                              ("pop10", ia.int32),
                                              ("pop_vec", ia.vector(2))])
        frame_json.drop_columns("data_lines")

        # Validate recovered vector components
        for row in frame_json.take(frame_json.row_count):
            # Check old and new populations.
            pop_vec = row[3]
            old_pop = row[2]
            new_pop = row[1]
            self.assertEqual(pop_vec[0], old_pop)
            self.assertEqual(pop_vec[1], new_pop)

    def test_vector_flatten(self):
        """flatten_column should expand vector"""
        self.frame.add_columns(lambda row: [row.rank+_ for _ in range(0, 3)],
                               ("count_vector", ia.vector(3)))
        row_orig = self.frame.row_count
        self.frame.flatten_columns('count_vector')
        row_flat = self.frame.row_count
        row_expect = row_orig * 3
        self.assertEqual(row_flat, row_expect)

    def test_vector_copy(self):
        """ Test simple frame copies. """
        # For the city with rank N, return a vector of N elements.
        self.frame.add_columns(lambda row: [row.rank+_ for _ in range(0, 3)],
                               ("count_vector", ia.vector(3)))
        pred_copy = self.frame.copy()
        src_copy = ia.Frame(self.frame)
        row_orig = self.frame.row_count
        self.frame.sort("rank")
        pred_copy.sort("rank")
        src_copy.sort("rank")
        orig_take = self.frame.take(row_orig)
        pred_take = pred_copy.take(row_orig)
        src_take = src_copy.take(row_orig)

        self.assertEqual(orig_take, pred_take)
        self.assertEqual(orig_take, src_take)

    def test_vector_join(self):
        """Join frames based on vector column"""
        self.frame.add_columns(
            lambda row: [row.rank*_/10.0 for _ in range(0, 3)],
            ("count_vector", ia.vector(3)))
        row_orig = self.frame.row_count
        # print self.frame.inspect(row_orig)
        left = self.frame.copy(columns={'city': 'metro',
                                        'count_vector': 'chits'})
        join_frame = left.join(self.frame, 'chits', 'count_vector', 'inner')
        # print join_frame.inspect(join_frame.row_count)

        self.assertEqual(row_orig, join_frame.row_count)

        # For a join implementation that drops the extra key column,
        #   subtract 1 from the first argument (expected value).
        self.assertEqual(len(left.schema)+len(self.frame.schema),
                         len(join_frame.schema))

    def test_vector_drop_dup(self):
        """Drop duplicate vectors"""
        # Produce 3-vectors with values based on ranking mod 7,
        #   both ascending and descending
        # This will produce only a handful of duplicates to drop
        self.frame.add_columns(
            lambda row: [(row.rank % 7)*_/10.0 for _ in range(0, 3)]
            if row.rank % 2
            else [(row.rank % 7)*i/10.0 for i in range(2, -1, -1)],
            ("count_vector", ia.vector(3)))
        # row_orig = self.frame.row_count
        # print self.frame.inspect(row_orig)
        self.frame.drop_duplicates('count_vector')
        # print self.frame.inspect(row_orig)
        # The first 7 rows match the last 7 and get dropped.
        self.assertEqual(self.frame.row_count, 13)

    def test_vector_filter(self):
        """Filter rows based on vector values"""
        # For the city with rank N, return a vector of N elements.
        self.frame.add_columns(
            lambda row: [row.rank*_ for _ in range(0, 3)],
            ("count_vector", ia.vector(3)))
        row_orig = self.frame.row_count
        # print self.frame.inspect(row_orig)
        self.frame.filter(lambda row: int(row.count_vector[1]) % 3 == 0)
        self.assertEqual(self.frame.row_count, row_orig//3)

    def test_vector_drop_rows(self):
        """Filter rows based on vector values"""
        self.frame.add_columns(
            lambda row: [row.rank*_ for _ in range(0, 3)],
            ("count_vector", ia.vector(3)))
        row_orig = self.frame.row_count
        self.frame.drop_rows(lambda row: int(row.count_vector[1]) % 3 == 0)
        self.assertEqual(self.frame.row_count, row_orig - row_orig//3)

    def test_vector_count(self):
        """Count vector elements"""
        self.frame.add_columns(
            lambda row: [(row.rank % 7)*_ for _ in range(0, 3)],
            ("count_vector", ia.vector(3)))
        # print self.frame.inspect(20)
        # count2 = self.frame.count(lambda row: row.count_vector ==
        #                           ia.vector(3)([0.0, 1.0, 2.0]))
        count2 = self.frame.count(lambda row: row.count_vector[1] == 1.0)
        self.assertEqual(count2, 3)

    def test_vector_group_by(self):
        """Group by vector column"""
        self.frame.add_columns(
            lambda row: [(row.rank % 7)*_ for _ in range(0, 3)],
            ("count_vector", ia.vector(3)))
        print self.frame.inspect(self.frame.row_count)
        row_orig = self.frame.row_count
        agg_frame = self.frame.group_by("count_vector", ia.agg.count)
        agg_frame.sort("count")
        print agg_frame.inspect()
        self.assertEqual(agg_frame.row_count, row_orig//3 + 1)
        agg_take = agg_frame.take(agg_frame.row_count)

        # There are two 0 rows; 3 each of the others.
        self.assertEqual(2, agg_take[0][1])
        self.assertEqual(3, agg_take[1][1])
        self.assertEqual(3, agg_take[agg_frame.row_count-1][1])

    def test_vector_top_k(self):
        """Find the most frequent items"""
        self.frame.add_columns(
            lambda row: [(30/row.rank)*_ for _ in range(0, 3)],
            ("count_vector", ia.vector(3)))
        top_frame = self.frame.top_k("count_vector", 3)
        # print top_frame.inspect()
        top_take = top_frame.take(top_frame.row_count)
        self.assertEqual(top_take[0][1], 5)
        self.assertEqual(top_take[1][1], 5)
        self.assertEqual(top_take[2][1], 3)

    def test_vector_entropy(self):
        """
        Compute the vector distribution's Shannon entropy.
        Correct value verified by hand calculation.
        """
        self.frame.add_columns(
            lambda row: [(30/row.rank)*_ for _ in range(0, 3)],
            ("count_vector", ia.vector(3)))
        entropy = self.frame.entropy("count_vector")
        print entropy
        self.assertAlmostEqual(entropy, 2.02622, 4)

    @unittest.skip("vector casting not implemented in Scala")
    def test_vector_col_mode(self):
        """Find the modes"""
        self.frame.add_columns(
            lambda row: [(row.rank % 7)*_ for _ in range(0, 3)],
            ("count_vector", ia.vector(3)))
        mode_dict = self.frame.column_mode("count_vector")
        print mode_dict
        # TODO: add assertions to validate this.

    @unittest.skip("vector casting not implemented in Scala")
    def test_vector_col_median(self):
        """Find the modes"""
        self.frame.add_columns(
            lambda row: [(row.rank % 7)*_ for _ in range(0, 3)],
            ("count_vector", ia.vector(3)))
        median = self.frame.column_median("count_vector")
        self.assertEqual(median, ia.vector(3)([0.0, 0.1, 0.2]))

    @unittest.skip("vector histogram facility not implemented")
    def test_vector_histogram(self):
        """Make a histogram of the vectors"""
        self.frame.add_columns(
            lambda row: [(row.rank % 7)*_ for _ in range(0, 3)],
            ("count_vector", ia.vector(3)))
        hist = self.frame.histogram("count_vector", 4)
        print hist
        # TODO: add assertions to validate this.

    @unittest.skip("vector.'<' not implemented")
    def test_vector_sort(self):
        """Sort on the vector column"""
        self.frame.add_columns(
            lambda row: [(row.rank % 7)*_ for _ in range(0, 3)],
            ("count_vector", ia.vector(3)))
        # row_orig = self.frame.row_count
        self.frame.sort("count_vector")
        print self.frame.inspect()
        # TODO: add assertions to validate this.

    @unittest.skip("vector.'<' not implemented")
    def test_vector_bin_col(self):
        """Bin on the vector column"""
        self.frame.add_columns(
            lambda row: [(row.rank % 7)*_ for _ in range(0, 3)],
            ("count_vector", ia.vector(3)))
        # row_orig = self.frame.row_count
        self.frame.bin_column_equal_depth("count_vector", 3, "bin_depth")
        self.frame.bin_column_equal_width("count_vector", 3, "bin_width")
        print self.frame.inspect()
        # TODO: add assertions to validate this.

    @unittest.skip("vector serialization not implemented")
    def test_vector_tally(self):
        """Find the most frequent items"""
        self.frame.add_columns(
            lambda row: [(30/row.rank)*_ for _ in range(0, 3)],
            ("count_vector", ia.vector(3)))
        self.frame.sort("change", False)
        self.frame.inspect(5)
        self.frame.tally("count_vector", ia.vector([0.0, 1.0, 2.0]))
        self.frame.tally_percent("count_vector", ia.vector([0.0, 1.0, 2.0]))
        # tally_take = self.frame.take(self.frame.row_count)
        # TODO: add assertions to validate this.

    def test_vector_download(self):
        """ Test conversion to PANDAS frame with vectors. """
        self.frame.add_columns(lambda row: [_ + row.rank for _ in range(0, 3)],
                               ("count_vector", ia.vector(3)))
        row_orig = self.frame.row_count
        print self.frame.inspect(row_orig)
        pandas_frame = self.frame.download()
        # print "PANDAS FRAME\n", pandas_frame

        vector_row_1 = pandas_frame['count_vector'].get_value(1, 1)
        self.assertEqual([2.0, 3.0, 4.0], vector_row_1)


if __name__ == "__main__":
    unittest.main()
