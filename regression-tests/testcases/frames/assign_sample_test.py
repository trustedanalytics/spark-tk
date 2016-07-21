""" Test assign sample functionality """

import unittest

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))
from qalib import sparktk_test


class AssignSample(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Build test frame"""
        super(AssignSample, self).setUp()
        schema = [("color1", str), ("predicted", str)]

        self.frame = self.context.frame.import_csv(
            self.get_file("model_color.csv"), schema=schema)

    def test_label_column(self):
        """Test splitting on the label column"""
        self.frame.assign_sample(
            [0.6, 0.3, 0.1], ['one', 'two', 'three'], 'label_column', 2)

        baseline = {'one': 0.6, 'two': 0.3, 'three': 0.1}
        self._test_frame_assign('label_column', baseline)

    def test_sample_bin(self):
        """Test splitting on the sample_bin column"""
        self.frame.assign_sample([0.5, 0.3, 0.2])
        baseline = {'TR': 0.5, 'TE': 0.3, 'VA': 0.2}
        self._test_frame_assign("sample_bin", baseline)

    def test_random_seed(self):
        """ Test seed is default 0, and non-0 is different """
        self.frame.assign_sample(
            [0.6, 0.2, 0.1, 0.1], output_column="default")
        self.frame.assign_sample(
            [0.6, 0.2, 0.1, 0.1], random_seed=0, output_column="seed_0")
        self.frame.assign_sample(
            [0.6, 0.2, 0.1, 0.1], random_seed=5, output_column="seed_5")
        baseline = {'Sample_0': 0.6,
                    'Sample_1': 0.2,
                    'Sample_2': 0.1,
                    'Sample_3': 0.1}
        # Check expected results
        self._test_frame_assign("default", baseline)
        frame_take = self.frame.take(self.frame.row_count)
        seed_d = [i[2] for i in frame_take.data]
        seed_0 = [i[3] for i in frame_take.data]
        seed_5 = [i[4] for i in frame_take.data]
        # seed=0 and default give the same results.
        self.assertEqual(seed_0, seed_d)
        # seed=0 and seed=5 give different assignments.
        self.assertNotEqual(seed_0, seed_5)

    def _test_frame_assign(self, column_name, sample):
        """Tests the assign method on the given column and sample"""
        pd = self.frame.download(self.frame.row_count)
        groupby_rows = pd.groupby(column_name).size()
        count = float(groupby_rows.sum())
        normalized = groupby_rows.map(lambda x: x/count)
        self.assertItemsEqual(normalized.keys(), sample.keys())
        for i, j in normalized.iteritems():
            self.assertAlmostEqual(sample[i], j, delta=0.1)


if __name__ == '__main__':
    unittest.main()


