"""Test cumulative tally functions, hand calculated baselines"""
import unittest
from sparktkregtests.lib import sparktk_test


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
        self.tally_frame = self.context.frame.import_csv(data_tally,
                schema=schema_tally)

    def test_tally_and_tally_percent(self):
        """Test tally and tally percent"""
        self.tally_frame.tally("rating", '5')
        self.tally_frame.tally_percent("rating", '5')

        pd_frame = self.tally_frame.download(self.tally_frame.row_count)
        for index, row in pd_frame.iterrows():
            self.assertAlmostEqual(
                row['percent_count'], row['rating_tally_percent'], delta=.0001)
            self.assertEqual(row['count'], row['rating_tally'])

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
                   u'rating_tally_1',
                   u'rating_tally_percent_1']

        self.assertItemsEqual(self.tally_frame.column_names, columns)

    def test_tally_no_column(self):
        """Test errors on non-existant column"""
        with self.assertRaisesRegexp(Exception, "Invalid column name"):
            self.tally_frame.tally("no_such_column", '5')

    def test_tally_no_column_percent(self):
        with self.assertRaisesRegexp(Exception, "Invalid column name"):
            self.tally_frame.tally_percent("no_such_column", '5')

    def test_tally_none(self):
        """Test tally none column errors"""
        with self.assertRaisesRegexp(Exception,
                "column name for sample is required"):
            self.tally_frame.tally(None, '5')

    def test_tally_none_percent(self):
        with self.assertRaisesRegexp(Exception,
                "column name for sample is required"):
            self.tally_frame.tally_percent(None, '5')

    def test_tally_bad_type(self):
        """Test tally on incorrect type errors"""
        with self.assertRaisesRegexp(Exception, "does not exist"):
            self.tally_frame.tally("rating", 5)

    def test_tally_bad_type_percent(self):
        with self.assertRaisesRegexp(Exception, "does not exist"):
            self.tally_frame.tally_percent("rating", 5)

    def test_tally_value_none(self):
        """Test tally on none errors"""
        with self.assertRaisesRegexp(Exception,
                "count value for the sample is required"):
            self.tally_frame.tally("rating", None)

    def test_tally_value_none_percent(self):
        with self.assertRaisesRegexp(Exception,
                "count value for the sample is required"):
            self.tally_frame.tally_percent("rating", None)

    def test_tally_no_element(self):
        """Test tallying on non-present element is correct"""
        self.tally_frame.tally_percent("rating", "12")
        local_frame = self.tally_frame.download(self.tally_frame.row_count)

        for index, row in local_frame.iterrows():
            self.assertEqual(row["rating_tally_percent"], 1.0)


if __name__ == '__main__':
    unittest.main()
