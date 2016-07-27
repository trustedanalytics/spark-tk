"""Test cumulative sum against known values"""
import unittest
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))

from qalib import sparktk_test


class TestCumulativeSum(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Build test frames"""
        super(TestCumulativeSum, self).setUp()

        data_sum = self.get_file("cumulative_seq_v2.csv")
        schema_sum = [("sequence", int),
                      ("col1", int),
                      ("cumulative_sum", int),
                      ("percent_sum", float)]
        self.sum_frame = self.context.frame.import_csv(data_sum, schema=schema_sum)

    def test_cumulative_sum_and_percent(self):
        """Test cumulative sum and cumulative percent"""
        self.sum_frame.cumulative_sum("col1")
        self.sum_frame.cumulative_percent("col1")

        pd_frame = self.sum_frame.download(self.sum_frame.row_count)
        for _, i in pd_frame.iterrows():
            self.assertAlmostEqual(
                i['cumulative_sum'], i['col1_cumulative_sum'], delta=.0001)
            self.assertAlmostEqual(
                i['percent_sum'], i['col1_cumulative_percent'], delta=.0001)

    def test_cumulative_colname_collision(self):
        """Test column name collision resolve gracefully"""
        # Repeatedly run cumulative functions to force collision
        self.sum_frame.cumulative_sum("col1")
        self.sum_frame.cumulative_percent("col1")

        self.sum_frame.cumulative_sum("col1")
        self.sum_frame.cumulative_percent("col1")

        self.sum_frame.cumulative_sum("col1")
        self.sum_frame.cumulative_percent("col1")

        new_columns = [u'sequence',
                       u'col1',
                       u'cumulative_sum',
                       u'percent_sum',
                       u'col1_cumulative_sum',
                       u'col1_cumulative_percent',
                       u'col1_cumulative_sum_0',
                       u'col1_cumulative_percent_0',
                       u'col1_cumulative_sum_0_1',
                       u'col1_cumulative_percent_0_1']
        self.assertItemsEqual(new_columns, self.sum_frame.column_names)

    def test_cumulative_bad_colname(self):
        """Test non-existant column errors"""
        with self.assertRaises(Exception):
            self.sum_frame.cumulative_sum("no_such_column")

        with self.assertRaises(Exception):
            self.sum_frame.cumulative_percent("no_such_column")

    def test_cumulative_none_column(self):
        """Test none for column errors"""
        with self.assertRaises(Exception):
            self.sum_frame.cumulative_sum(None)

        with self.assertRaises(Exception):
            self.sum_frame.cumulative_percent(None)


if __name__ == '__main__':
    unittest.main()
