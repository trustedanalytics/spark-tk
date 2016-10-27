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

""" test cases for LDA implementation """
import unittest
from sparktkregtests.lib import sparktk_test
from sparktkregtests.lib import scoring_utils
from ConfigParser import SafeConfigParser


class LDAModelTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Set LDA frame."""
        super(LDAModelTest, self).setUp()

        schema = [('paper', str),
                  ('word', str),
                  ('count', int),
                  ('topic', str)]
        self.lda_frame = self.context.frame.import_csv(self.get_file("lda8.csv"), schema=schema)
        self.config = SafeConfigParser()
        self.config.read('../../lib/port.ini')

    def test_model_scoring(self):
        """Test lda model scoring"""
        model = self.context.models.clustering.lda.train(self.lda_frame, 'paper', 'word', 'count',
                              num_topics=5, max_iterations=10, seed=0)

        test_phrase = ["word-0-0", "word-1-0",
                       "word-2-0", "word-3-0", "word-4-0"]

        file_name = self.get_name("lda")
        model_path = model.export_to_mar(self.get_export_file(file_name))

        res = lda_model.predict(test_phrase)["topics_given_doc"]

        with scoring_utils.scorer(
                model_path, self.config.get('port', self.id())) as scorer:
            result = scorer.score([{"paper":test_phrase}]).json()
            for i, j in zip(res, result[u"data"][0]["topics_given_doc"]):
                self.assertAlmostEqual(i, j)


if __name__ == '__main__':
    unittest.main()
