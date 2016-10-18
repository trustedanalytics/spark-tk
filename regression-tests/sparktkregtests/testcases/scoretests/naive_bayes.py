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

""" Tests Naive Bayes Model against known values.  """
import unittest
import time

from sparktkregtests.lib import sparktk_test
from sparktkregtests.lib import scoring_utils


class NaiveBayes(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Build the frames needed for the tests."""
        super(NaiveBayes, self).setUp()

        dataset = self.get_file("naive_bayes.csv")
        schema = [("label", int),
                  ("f1", int),
                  ("f2", int),
                  ("f3", int)]
        self.frame = self.context.frame.import_csv(dataset, schema=schema)

    def test_model_basic(self):
        """Test training intializes theta, pi and labels"""
        model = self.context.models.classification.naive_bayes.train(self.frame, "label", ['f1', 'f2', 'f3'])

        res = model.predict(self.frame, ['f1', 'f2', 'f3'])

        analysis = self.frame.to_pandas()
        model_path = model.export_to_mar(self.get_export_file("naive_bayes"))
        with scoring_utils.scorer(model_path) as scorer:
            time.sleep(10)
            for _, i in analysis.iterrows():
                r = scorer.score([dict(zip(['f1', 'f2', 'f3'], list(i[1:4])))])
                self.assertEqual(r.json()["data"][0]['Score'], i[4])


if __name__ == '__main__':
    unittest.main()
