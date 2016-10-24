# vim: set encoding=utf-8

#  Copyright (c) 2016 Intel Corporation 
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

""" Test assign sample functionality """

import unittest
import sys
import os
from sparktkregtests.lib import sparktk_test


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

    def test_seed(self):
        """ Test seed is default 0, and non-0 is different """
        self.frame.assign_sample(
            [0.6, 0.2, 0.1, 0.1], output_column="default")
        self.frame.assign_sample(
            [0.6, 0.2, 0.1, 0.1], seed=0, output_column="seed_0")
        self.frame.assign_sample(
            [0.6, 0.2, 0.1, 0.1], seed=5, output_column="seed_5")
        baseline = {'Sample_0': 0.6,
                    'Sample_1': 0.2,
                    'Sample_2': 0.1,
                    'Sample_3': 0.1}

        # Check expected results
        self._test_frame_assign("default", baseline)
        frame_take = self.frame.take(self.frame.count())
        seed_d = [i[2] for i in frame_take]
        seed_0 = [i[3] for i in frame_take]
        seed_5 = [i[4] for i in frame_take]

        # seed=0 and default give the same results.
        self.assertEqual(seed_0, seed_d)

        # seed=0 and seed=5 give different assignments.
        self.assertNotEqual(seed_0, seed_5)

    def _test_frame_assign(self, column_name, sample):
        """Tests the assign method on the given column and sample"""
        pd = self.frame.to_pandas(self.frame.count())
        groupby_rows = pd.groupby(column_name).size()
        count = float(groupby_rows.sum())
        normalized = groupby_rows.map(lambda x: x/count)
        self.assertItemsEqual(normalized.keys(), sample.keys())
        for i, j in normalized.iteritems():
            self.assertAlmostEqual(sample[i], j, delta=0.1)


if __name__ == '__main__':
    unittest.main()
