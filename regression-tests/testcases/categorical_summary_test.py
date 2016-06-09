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
    Usage:  python2.7 categorical_summary_test.py
    Test summary statistics for both numerical and categorical columns.
"""
__author__ = 'Karen Herrold'
__credits__ = ["Karen Herrold"]
__version__ = "2015.07.14"

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest

import numpy as np
import pandas as pd
pd.set_option('precision', 16)
import trustedanalytics as ia

from qalib import frame_utils
from qalib import atk_test


class CategoricalSummaryTest(atk_test.ATKTestCase):

    def setUp(self):
        super(CategoricalSummaryTest, self).setUp()

        # Create basic movie graph.
        datafile = "movie_small.csv"

        # Define csv file.
        self.schema = [('movie', unicode),
                       ('vertexType', unicode),
                       ('user', unicode),
                       ('rating', ia.int32),
                       ('splits', unicode),
                       ('percent', ia.int32),
                       ('predict', ia.int32),
                       ('weight', ia.int32),
                       ('age', ia.int32)]

        # Create frame.
        self.frame = frame_utils.build_frame(
            datafile, self.schema, skip_header_lines=1, prefix=self.prefix)

    def _calc_pandas_summary(self, params):
        # Use pandas to create expected result.
        pf = self.frame.download(self.frame.row_count)
        num_rows = len(pf.index)

        # Create return structure.
        l = []
        d = {u'categorical_summary': l}

        for item in params:
            if 'column_name' in item:
                column = item['column_name']
            else:
                print "Requires a column name to be specified."
                return None

            # Set defaults.
            k = 10
            threshold = 0

            # If specified, update values.
            if 'top_k' in item:
                k = item['top_k']

            if 'threshold' in item:
                threshold = item['threshold']
                if 'top_k' not in item:
                    k = num_rows

            # Build summary from data frame.
            counts = pf[column].value_counts()[:k]
            df = pd.DataFrame(counts,
                              columns=[u'frequency'])
            df.reset_index(level=0, inplace=True)
            df.rename(columns={'index': u'level'}, inplace=True)
            df.sort([u'frequency', u'level'], ascending=False, inplace=True)
            df.reset_index(level=0, drop=True, inplace=True)
            df[u'percentage'] = (df.frequency / num_rows).astype(float)
            df1 = df[df.percentage > threshold]

            # Calculate missing & other.
            missing = np.sum(pf[column].isnull())
            other = num_rows - df1.frequency.sum() - missing
            df2 = pd.DataFrame([{u'level': u'Missing',
                                 u'frequency': missing,
                                 u'percentage': float(missing) / num_rows},
                                {u'level': u'Other',
                                 u'frequency': other,
                                 u'percentage': float(other) / num_rows}])

            # Convert dtype of level to unicode.
            # df1['level'] = df1['level'].apply(lambda level: unicode(level))
            df1['level'] = df['level'].astype(unicode)

            # Append df2 to original frame.  Unfortunately, this changes
            # column order.
            df3 = df1.append(df2, ignore_index=True)

            # Convert pandas frame to dictionary.
            levels = df3.T.to_dict().values()

            # Append this item to return structure.
            x = {u'column': unicode(column, "utf-8"),
                 u'levels': levels}
            l.append(x)

        return d

    def test_cat_summary_single_column_defaults(self):
        """
        Happy path test for categorical summary using defaults.
        """
        stats = self.frame.categorical_summary('age')

        # Using an explicit list here since there's a two-way tie for 10th
        # place and ATK algorithm finds one value and pandas finds the other.
        # Values were confirmed using pandas.  If anyone knows how to make
        # this work, I'd love to hear from you.
        expected_results = \
            {u'categorical_summary':
                [{u'column':  u'age',
                  u'levels': [{u'frequency': 15,
                               u'level': u'30',
                               u'percentage': 0.02512562814070352},
                              {u'frequency': 14,
                               u'level': u'72',
                               u'percentage': 0.023450586264656615},
                              {u'frequency': 13,
                               u'level': u'79',
                               u'percentage': 0.021775544388609715},
                              {u'frequency': 13,
                               u'level': u'68',
                               u'percentage': 0.021775544388609715},
                              {u'frequency': 13,
                               u'level': u'66',
                               u'percentage': 0.021775544388609715},
                              {u'frequency': 13,
                               u'level': u'64',
                               u'percentage': 0.021775544388609715},
                              {u'frequency': 13,
                               u'level': u'58',
                               u'percentage': 0.021775544388609715},
                              {u'frequency': 13,
                               u'level': u'36',
                               u'percentage': 0.021775544388609715},
                              {u'frequency': 13,
                               u'level': u'31',
                               u'percentage': 0.021775544388609715},
                              {u'frequency': 12,
                               u'level': u'67',
                               u'percentage': 0.020100502512562814},
                              {u'frequency': 0,
                               u'level': u'Missing',
                               u'percentage': 0.0},
                              {u'frequency': 465,
                               u'level': u'Other',
                               u'percentage': 0.7788944723618091}]}]}

        self.assertDictEqual(expected_results,
                             stats,
                             msg="Dictionary 'expected_results' does not "
                                 "match dictionary 'stats': defaults.\n"
                                 "pandas_results:\n{0}\n\nstats:\n{1}"
                             .format(expected_results, stats))

    @unittest.skip("pandas update broke this, fixed in private branch")
    def test_cat_summary_single_column_top_k(self):
        """
        Happy path test for categorical summary using top_k.
        """
        # k was picked to avoid ties in the data as the two algorithms handle
        # ties differently.  ATK sorts values in descending order within the
        # group while pandas uses ascending order.
        stats = self.frame.categorical_summary(('age', {'top_k': 11}))

        # Generate expected results using pandas for comparison.
        pandas_results = self._calc_pandas_summary([{'column_name': 'age',
                                                     'top_k': 11}])

        self.assertDictEqual(pandas_results,
                             stats,
                             msg="Dictionary 'pandas_results' does not "
                                 "match dictionary 'stats': top_k only.\n"
                                 "pandas_results:\n{0}\n\nstats:\n{1}"
                             .format(pandas_results, stats))

    def test_cat_summary_single_column_threshold(self):
        """
        Happy path test for categorical summary using threshold.
        """
        stats = self.frame.categorical_summary(('age',
                                                {"threshold": .02}))

        # Generate results using pandas for comparison.
        pandas_results = self._calc_pandas_summary([{'column_name': 'age',
                                                     'threshold': 0.02}])

        self.assertEqual(set(pandas_results),
                         set(stats),
                         msg="stats does not contain the same information as"
                             "pandas_results: stats:\n{0}\n\npandas_results:\n"
                             "{1}".format(stats, pandas_results))

    def test_cat_summary_single_column_top_k_and_threshold(self):
        """
        Happy path test for categorical summary using top_k & threshold.
        """
        stats = self.frame.categorical_summary(('age',
                                               {"threshold": .02,
                                                "top_k": 11}))

        # Generate results using pandas for comparison.
        pandas_results = self._calc_pandas_summary([{'column_name': 'age',
                                                     'top_k': 11,
                                                     'threshold': 0.02}])

        # Using set compare here because list not in same order.
        self.assertEqual(set(pandas_results),
                         set(stats),
                         msg="stats does not contain the same information as"
                             "pandas_results: stats:\n{0}\n\npandas_results:\n"
                             "{1}".format(stats, pandas_results))

    def test_cat_summary_single_column_with_None(self):
        """
        Test for categorical summary using top_k with Nones in data.
        """
        def add_nones(row):
            if row['age'] % 5 == 0:      # ending in 0 or 5
                return np.inf
            if row['age'] % 10 == 9:     # ending in 9
                return -np.inf
            if row['age'] % 13 == 0:    # multiple of 13
                return np.nan
            return row['age']

        # Add a column with +/- infinity & NaN.  Has to be a float because
        # NumPy doesn't support None as an int.
        self.frame.add_columns(add_nones, ('Nones', ia.float64))

        # k was picked to avoid ties in the data as the two algorithms handle
        # ties differently.  ATK sorts values in descending order within the
        # group while pandas uses ascending order.
        stats = self.frame.categorical_summary(('Nones', {'top_k': 8}))

        # Using previously constructed results since pandas top n
        # functionality does not seem to operate as expected when there is
        # a NaN in the column.  Values were confirmed with pandas calculation.
        # If anyone knows how to make this work, I'd love to hear from you.
        expected_results = \
            {u'categorical_summary':
             [{u'column': u'Nones',
               u'levels': [{u'percentage': 0.023450586264656615,
                            u'frequency': 14,
                            u'level': u'72.0'},
                           {u'percentage': 0.021775544388609715,
                            u'frequency': 13,
                            u'level': u'68.0'},
                           {u'percentage': 0.021775544388609715,
                            u'frequency': 13,
                            u'level': u'66.0'},
                           {u'percentage': 0.021775544388609715,
                            u'frequency': 13,
                            u'level': u'64.0'},
                           {u'percentage': 0.021775544388609715,
                            u'frequency': 13,
                            u'level': u'58.0'},
                           {u'percentage': 0.021775544388609715,
                            u'frequency': 13,
                            u'level': u'36.0'},
                           {u'percentage': 0.021775544388609715,
                            u'frequency': 13,
                            u'level': u'31.0'},
                           {u'percentage': 0.020100502512562814,
                            u'frequency': 12,
                            u'level': u'67.0'},
                           {u'percentage': 0.35175879396984927,
                            u'frequency': 210,
                            u'level': u'Missing'},
                           {u'percentage': 0.474036850921273,
                            u'frequency': 283,
                            u'level': u'Other'}]}]}
        self.assertDictEqual(expected_results,
                             stats,
                             msg="Dictionary 'pandas_results' does not "
                                 "match dictionary 'stats': top_k only.\n"
                                 "expected_results:\n{0}\n\nstats:\n{1}"
                             .format(expected_results, stats))

        # This is left in, but commented out, for now to check the comparison
        # data.  Hopefully, we will be able to generate it in the near future.
        # # Generate expected results using pandas for comparison.
        # pandas_results = self._calc_pandas_summary([{'column_name': 'Nones',
        #                                              'top_k': 8}])
        # self.assertDictEqual(pandas_results,
        #                      stats,
        #                      msg="Dictionary 'pandas_results' does not "
        #                          "match dictionary 'stats': top_k only.\n"
        #                          "pandas_results:\n{0}\n\nstats:\n{1}"
        #                      .format(pandas_results, stats))

    def test_cat_summary_multi_column(self):
        """
        Happy path test for categorical summary using multiple columns.
        """
        stats = self.frame.categorical_summary('movie',
                                               ('user', {'top_k': 10}),
                                               ('age', {'threshold': 0.02}),
                                               ('rating', {'top_k': 10,
                                                           'threshold': 0.02}),
                                               ('weight', {'top_k': 10}))

        # Generate results using pandas for comparison.
        pandas_results = self._calc_pandas_summary([{'column_name': 'movie'},
                                                    {'column_name': 'user',
                                                     'top_k': 10},
                                                    {'column_name': 'age',
                                                     'threshold': 0.02},
                                                    {'column_name': 'rating',
                                                     'top_k': 10,
                                                     'threshold': 0.02},
                                                    {'column_name': 'weight'}])
        print "stats:\n{0}".format(stats)
        print "pandas_results:\n{0}".format(pandas_results)

        # Using set compare here because lists are not in same order.
        self.assertEqual(set(pandas_results),
                         set(stats),
                         msg="stats does not contain the same information as"
                             "pandas_results: stats:\n{0}\n\npandas_results:\n"
                             "{1}".format(stats, pandas_results))

    def test_cat_summary_invalid_column_name_error(self):
        """
        Error path test for categorical summary - bad column name.
        """
        self.assertRaises(ia.rest.command.CommandServerError,
                          self.frame.categorical_summary,
                          ('invalid', {"threshold": .02}))

    def test_cat_summary_invalid_threshold_error_lo(self):
        """
        Error path test for categorical summary - invalid lower threshold.
        """
        self.assertRaises(ia.rest.command.CommandServerError,
                          self.frame.categorical_summary,
                          ('age', {"threshold": -1}))

    def test_cat_summary_invalid_threshold_error_hi(self):
        """
        Error path test for categorical summary - invalid upper threshold.
        """
        self.assertRaises(ia.rest.command.CommandServerError,
                          self.frame.categorical_summary,
                          ('age', {"threshold": 1.1}))

    def test_cat_summary_invalid_top_k_error(self):
        """
        Error path test for categorical summary - invalid top_k.
        """
        self.assertRaises(ia.rest.command.CommandServerError,
                          self.frame.categorical_summary,
                          ('age', {"top_k": -1}))

if __name__ == "__main__":
    unittest.main()
