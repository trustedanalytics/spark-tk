""" Tests weighted median functionality."""

import unittest
from sparktkregtests.lib import sparktk_test


class WeightedMedians(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Create input and baselines before run."""
        super(WeightedMedians, self).setUp()
        dataset1 = self.get_file("weight_median.csv")
        dataset2 = self.get_file("weighted_median_negative.csv")

        self.frame_median = self.context.frame.import_csv(dataset1,
                schema=[("x0", int), ("x1", int)])
        self.frame_median2 = self.context.frame.import_csv(dataset2,
                schema=[("x0", int), ("x1", int), ("x2", int)])

    def test_non_weighted_median(self):
        """Non weighted median calculation on smaller dataset"""
        weighted_median = self.frame_median.column_median('x0')
        self.assertEqual(weighted_median, 499)

    def test_weighted_median_small_data(self):
        """Weighted median on smaller dataset"""
        weighted_median = self.frame_median.column_median('x0', 'x1')
        self.assertEqual(weighted_median, 706)

    def test_non_weighted_large_data(self):
        """ Non weighted median calculation on larger dataset"""
        weighted_median = self.frame_median2.column_median('x0')
        self.assertEqual(weighted_median, 49999)

    def test_weighted_median_large_data(self):
        """Weighted median on larger dataset"""
        weighted_median = self.frame_median2.column_median('x0', 'x1')
        self.assertEqual(weighted_median, 29289)

    @unittest.skip("Weighted Median with Negative Weights does not error")
    def test_weighted_median_negative_weights(self):
        """Weighted median calculation where weights are negative integers"""
        weighted_median = self.frame_median2.column_median('x0', 'x2')
        self.assertEqual(weighted_median, None)


if __name__ == '__main__':
    unittest.main()
