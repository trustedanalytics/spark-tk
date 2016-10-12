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

"""validate Latent Dirchlet allocation against a known dataset """
import unittest
from sparktkregtests.lib import sparktk_test


def max_index(list):
    """Returns the index of the max value of list."""
    split_list = zip(list, range(len(list)))
    (retval, retI) = reduce(lambda (currV, currI), (nV, nI):
                            (currV, currI) if currV > nV
                            else (nV, nI), split_list)
    return retI


class LDAModelTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Set LDA frame."""
        super(LDAModelTest, self).setUp()

        schema = [('paper', str),
                  ('word', str),
                  ('count', long),
                  ('topic', str)]
        self.lda_frame = self.context.frame.import_csv(
            self.get_file("lda8.csv"), schema=schema)

    def test_lda_model_count(self):
        """Test train method for the LDA Model"""
        lda_model = self.context.models.clustering.lda.train(
            self.lda_frame, 'paper', 'word', 'count', num_topics=5,
            max_iterations=40, alpha=[-1.0], check_point_interval=1)
        self._confirm_model_valid(lda_model)


    def test_lda_predict(self):
        """Standard test for the LDA Model predict."""
        # Add a new column
        self.lda_frame.add_columns(lambda row: row.count,
                                   ('count32', int))

        lda_model = self.context.models.clustering.lda.train(
            self.lda_frame, 'paper', 'word', 'count32',
            num_topics=5, max_iterations=50, random_seed=5,
            alpha=[-1.0], check_point_interval=1)

        predict_vals1 = lda_model.predict(
            ['word-0-0', 'word-1-0',
             'word-2-0', 'word-3-0', 'word-4-0'])["topics_given_doc"]
        predict_vals2 = lda_model.predict(
            ['word-5-0', 'word-6-0',
             'word-7-0', 'word-8-0', 'word-9-0'])["topics_given_doc"]
        for i in zip(predict_vals1, predict_vals2):
            self.assertAlmostEqual(i[0], i[1])

    def _confirm_model_valid(self, model):
        """Validates the results of lda train."""
        # fetch all the words, and all the docs
        report = model.report
        self.assertIn("maxIterations", report)

        word_topic_res = model.word_given_topics_frame
        topic_words_res = model.topics_given_word_frame
        topic_doc_res = model.topics_given_doc_frame
        words = word_topic_res.take(word_topic_res.count())
        topics = topic_words_res.take(topic_words_res.count())
        docs = topic_doc_res.take(topic_doc_res.count())

        baseline_words = filter(lambda x: x[0][-1] != '-', words)
        base_topics = filter(lambda x: x[0][-1] != '-', topics)

        # get the baseline values
        all_rows = self.lda_frame.take(self.lda_frame.count())

        # filter out the common words and data lines
        baseline_rows = filter(lambda x: x[3] != "-1", all_rows)

        # associate each word/doc with their topic
        # use set to remove duplicates
        words_topic = list(set(map(lambda x: (x[1], x[3]), baseline_rows)))

        # group the words by topic
        words_grouped = map(lambda topic:
                            map(lambda (a, b):
                                a, filter(lambda (a, b):
                                          b == str(topic), words_topic)),
                            range(5))

        docs_topic = list(set(map(lambda x: (x[0], x[3]), baseline_rows)))
        docs_grouped = map(lambda topic:
                           map(lambda (a, b):
                               a, filter(lambda (a, b):
                                         b == str(topic), docs_topic)),
                           range(5))

        # check the grouped baseline docs all have the same index val
        # maxed in their associated doc
        for i in docs_grouped:
            grouped_docs = filter(lambda a: a[0] in i, docs)
            positions = map(lambda x: max_index(map(float, x[1])),
                            grouped_docs)
            res = {}
            total = 0.0
            for i in positions:
                total = total + 1.0
                if i in res:
                    res[i] = res[i] + 1.0
                else:
                    res[i] = 1.0
            self.assertGreater(max(res.values())/total, .9)

        # check the grouped baseline words all have the same index val maxed
        # in their associated word
        for i in words_grouped:
            grouped_words = filter(lambda a: a[0] in i, baseline_words)
            positions = map(lambda x: max_index(map(float, x[1])),
                            grouped_words)
            res = {}
            total = 0.0
            for i in positions:
                total = total + 1.0
                if i in res:
                    res[i] = res[i] + 1.0
                else:
                    res[i] = 1.0
            self.assertGreater(max(res.values())/total, .5)

        for i in words_grouped:
            grouped_words = filter(lambda a: a[0] in i, base_topics)
            positions = map(lambda x: max_index(map(float, x[1])),
                            grouped_words)
            res = {}
            total = 0.0
            for i in positions:
                total = total + 1.0
                if i in res:
                    res[i] = res[i] + 1.0
                else:
                    res[i] = 1.0
            self.assertGreater(max(res.values())/total, .5)

    def test_lda_frame_none(self):
        """Test training on a None frame errors."""
        with(self.assertRaisesRegexp(
                Exception, ".*NoneType.*")):
            lda_model = self.context.models.clustering.lda.train(
                None, 'paper', 'word', 'count', num_topics=5)

    def test_lda_column_bad_type(self):
        """Test passing bad type to docs column."""
        with(self.assertRaisesRegexp(
                Exception, "Method .* does not exist")):
            self.context.models.clustering.lda.train(
                self.lda_frame, 5, 'word', 'count', num_topics=5)

    def test_lda_max_iterations_negative(self):
        """Test max_iterations with a negative value."""
        with(self.assertRaisesRegexp(
                Exception, "Max iterations should be greater than 0")):
            self.context.models.clustering.lda.train(
                self.lda_frame, 'paper', 'word', 'count', num_topics=5,
                max_iterations=-6)

    def test_lda_negative_topics(self):
        """Test num_topics with a negative value."""
        with(self.assertRaisesRegexp(
                Exception, "Number of topics \(K\) should be greater than 0")):
            self.context.models.clustering.lda.train(
                self.lda_frame, 'paper', 'word', 'count',
                num_topics=-5)

    def test_lda_alpha_negative(self):
        """Test lda with a negative alpha < -1"""
        with(self.assertRaisesRegexp(
                Exception, "Alpha should be greater than 1.0. Or -1.0 .*")):
            self.context.models.clustering.lda.train(
                self.lda_frame, 'paper', 'word', 'count',
                num_topics=5, alpha=[-.1])

    def test_lda_seed_type(self):
        """Test lda with a bad seed type."""
        with(self.assertRaisesRegexp(
                Exception, "invalid literal .* \'foo\'")):
            self.context.models.clustering.lda.train(
                self.lda_frame, 'paper', 'word', 'count',
                num_topics=5, random_seed="foo")

    def test_lda_no_random_seed(self):
        """Test lda with a fixed random seed"""
        self.context.models.clustering.lda.train(
            self.lda_frame, 'paper', 'word', 'count',
            num_topics=5, random_seed=0, max_iterations=20,
            alpha=[-1.0], check_point_interval=1)

    @unittest.skip("not implemented")
    def test_lda_publish(self):
        """Test training for more iterations."""
        lda_model = self.context.models.clustering.lda.train(
            self.lda_frame, 'paper', 'word', 'count',
            num_topics=5, max_iterations=30, random_seed=5,
            check_point_interval=1)
        path = lda_model.publish()
        self.assertIn("hdfs", path)
        self.assertIn("tar", path)

    def test_lda_max_iterations(self):
        """Test training for more iterations."""
        lda_model = self.context.models.clustering.lda.train(
            self.lda_frame, 'paper', 'word', 'count', num_topics=5,
            max_iterations=30, random_seed=5, alpha=[-1.0],
            check_point_interval=1)

        self._confirm_model_valid(lda_model)

    def test_lda_alpha_beta_large(self):
        """Test alpha and beta behave correctly."""

        lda_model = self.context.models.clustering.lda.train(
            self.lda_frame, 'paper', 'word', 'count',
            num_topics=5, alpha=[10000000.0], beta=10000000.0,
            max_iterations=20, check_point_interval=1)

        self._confirm_model_valid(lda_model)

        # test the lists are completely uniform (as expected)
        word_report = lda_model.word_given_topics_frame
        words = word_report.take(word_report.count())

        doc_results = lda_model.topics_given_doc_frame
        docs = doc_results.take(word_report.count())

        topic_results = lda_model.topics_given_word_frame
        topics = topic_results.take(word_report.count())

        doc_split = map(lambda x: map(float, x[1]), docs)

        words_split = map(lambda x: map(float, x[1]), words)

        topics_split = map(lambda x: map(float, x[1]), topics)

        map(lambda x: map(lambda y: self.assertAlmostEquals(x[0], y), x),
            doc_split)
        map(lambda x: map(lambda y: self.assertAlmostEquals(x[0], y), x),
            words_split)
        map(lambda x: map(lambda y: self.assertAlmostEquals(x[0], y), x),
            topics_split)


if __name__ == '__main__':
    unittest.main()
