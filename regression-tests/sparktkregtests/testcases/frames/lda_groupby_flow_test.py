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

"""Sample LDA/Groupby example"""
import unittest
from sparktkregtests.lib import sparktk_test
import numpy


class LDAExample(sparktk_test.SparkTKTestCase):

    def test_lda_example(self):
        """LDA demo from examples directory"""

        # this is a full worked example of lda and groupby
        # with known correct values
        data = [['nytimes', 'harry', 3], ['nytimes', 'economy', 35], ['nytimes', 'jobs', 40], ['nytimes', 'magic', 1],
                ['nytimes', 'realestate', 15], ['nytimes', 'movies', 6], ['economist', 'economy', 50],
                ['economist', 'jobs', 35], ['economist', 'realestate', 20], ['economist', 'movies', 1],
                ['economist', 'harry', 1], ['economist', 'magic', 1], ['harrypotter', 'harry', 40],
                ['harrypotter', 'magic', 30], ['harrypotter', 'chamber', 20], ['harrypotter', 'secrets', 30]]
        frame = self.context.frame.create(
            data,
            schema=[('doc_id', str),
                    ('word_id', str),
                    ('word_count', long)])

        model = self.context.models.clustering.lda.train(
                frame, "doc_id", "word_id", "word_count", max_iterations=3, num_topics=2)

        doc_results = model.topics_given_doc_frame
        word_results = model.word_given_topics_frame

        doc_results.rename_columns({'topic_probabilities': 'lda_results_doc'})
        word_results.rename_columns(
            {'topic_probabilities': 'lda_results_word'})

        frame = frame.join_left(
            doc_results, left_on="doc_id", right_on="doc_id")
        frame = frame.join_left(
            word_results, left_on="word_id", right_on="word_id")

        frame.dot_product(
            ['lda_results_doc'], ['lda_results_word'], 'lda_score')

        word_hist = frame.histogram('word_count', 4)
        lda_hist = frame.histogram('lda_score', 2)

        group_frame = frame.group_by(
            'word_id_L',
            {'word_count': self.context.agg.histogram(
                cutoffs=word_hist.cutoffs,
                include_lowest=True,
                strict_binning=False),
             'lda_score': self.context.agg.histogram(lda_hist.cutoffs)})
        pandas = group_frame.to_pandas()

        for (index, row) in pandas.iterrows():
            if str(row["word_id_L"]) == "magic":
                numpy.testing.assert_equal(list(row["word_count_HISTOGRAM"]), [float(2.0/3.0), 0, float(1.0/3.0), 0])


if __name__ == "__main__":
    unittest.main()
