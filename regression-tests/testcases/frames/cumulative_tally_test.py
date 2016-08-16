"""Test cumulative tally functions, hand calculated baselines"""
import unittest
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))
from qalib import sparktk_test


class TestCumulativeTally(sparktk_test.SparkTKTestCase):

    def setUp(self):
        super(TestCumulativeTally, self).setUp()

        data_tally = self.get_file("cumu_tally_seq.csv")
        schema_tally = [("sequence", int),
                        ("user_id", int),
                        ("vertex_type", str),
                        ("movie_id", int),
                        ("rating", int),
                        ("splits", str),
                        ("count", int),
                        ("percent_count", float)]
        self.tally_frame = self.context.frame.import_csv(data_tally, schema=schema_tally)

    def test_tally_and_tally_percent(self):
        """Test tally and tally percent"""
        self.tally_frame.tally("rating", '5')
        self.tally_frame.tally_percent("rating", '5')

        pd_frame = self.tally_frame.download(self.tally_frame.row_count)
        for _, i in pd_frame.iterrows():
            self.assertAlmostEqual(
                i['percent_count'], i['rating_tally_percent'], delta=.0001)
            self.assertEqual(i['count'], i['rating_tally'])

    def test_tally_colname_collision(self):
        """Test tally column names collide gracefully"""

        # repeatedly run tally to force collisions
        self.tally_frame.tally("rating", '5')
        self.tally_frame.tally_percent("rating", '5')

        self.tally_frame.tally("rating", '5')
        self.tally_frame.tally_percent("rating", '5')

        self.tally_frame.tally("rating", '5')
        self.tally_frame.tally_percent("rating", '5')
        columns = [u'sequence',
                   u'user_id',
                   u'vertex_type',
                   u'movie_id',
                   u'rating',
                   u'splits',
                   u'count',
                   u'percent_count',
                   u'rating_tally',
                   u'rating_tally_percent',
                   u'rating_tally_0',
                   u'rating_tally_percent_0',
                   u'rating_tally_0_1',
                   u'rating_tally_percent_0_1']

        self.assertItemsEqual(self.tally_frame.column_names, columns)

    def test_tally_no_column(self):
        """Test errors on non-existant column"""
        with self.assertRaises(Exception):
            self.tally_frame.tally("no_such_column", '5')

        with self.assertRaises(Exception):
            self.tally_frame.tally_percent("no_such_column", '5')

    def test_tally_none(self):
        """Test tally none column errors"""
        with self.assertRaises(Exception):
            self.tally_frame.tally(None, '5')

        with self.assertRaises(Exception):
            self.tally_frame.tally_percent(None, '5')

    def test_tally_bad_type(self):
        """Test tally on incorrect type errors"""
        with self.assertRaises(Exception):
            self.tally_frame.tally("rating", 5)

        with self.assertRaises(Exception):
            self.tally_frame.tally_percent("rating", 5)

    def test_tally_value_none(self):
        """Test tally on none errors"""
        with self.assertRaises(Exception):
            self.tally_frame.tally("rating", None)

        with self.assertRaises(Exception):
            self.tally_frame.tally_percent("rating", None)

    def test_tally_no_element(self):
        """Test tallying on non-present element is correct"""
        self.tally_frame.tally_percent("rating", "12")
        local_frame = self.tally_frame.download(self.tally_frame.row_count)

        for _, i in local_frame.iterrows():
            self.assertEqual(i["rating_tally_percent"], 1.0)


if __name__ == '__main__':
    unittest.main()
