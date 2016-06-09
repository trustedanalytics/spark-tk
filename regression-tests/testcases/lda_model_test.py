##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2014, 2015 Intel Corporation All Rights Reserved.
#
# The source code contained or described herein and all documents related to
# the source code (Material) are owned by Intel Corporation or its suppliers
# or licensors. Title to the Material remains with Intel Corporation or its
# suppliers and licensors. The Material may contain trade secrets and
# proprietary and confidential information of Intel Corporation and its
# suppliers and licensors, and is protected by worldwide copyright and trade
# secret laws and treaty provisions. No part of the Material may be used,
# copied, reproduced, modified, published, uploaded, posted, transmitted,
# distributed, or disclosed in any way without Intel's prior express written
# permission.
#
# No license under any patent, copyright, trade secret or other intellectual
# property right is granted to or conferred upon you by disclosure or
# delivery of the Materials, either expressly, by implication, inducement,
# estoppel or otherwise. Any license under such intellectual property rights
# must be express and approved by Intel in writing.
##############################################################################
""" test cases for LDA implementation
    usage: python2.7 lda_model_test.py
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest

import trustedanalytics as ia

from qalib import frame_utils
from qalib import common_utils
from qalib import atk_test


def max_index(list):
    """Returns the index of the max value of list."""
    split_list = zip(list, range(len(list)))
    (retval, retI) = reduce(lambda (currV, currI), (nV, nI):
                            (currV, currI) if currV > nV
                            else (nV, nI), split_list)
    return retI


class LDAModelTest(atk_test.ATKTestCase):

    def setUp(self):
        """Set LDA frame."""
        super(LDAModelTest, self).setUp()

        schema = [('paper', str),
                  ('word', str),
                  ('count', ia.int64),
                  ('topic', str)]
        self.lda_frame = frame_utils.build_frame(
            "lda8.csv", schema, self.prefix)

    def test_lda_model_int64_count(self):
        """Standard test for the LDA Model code using int64 count."""
        name = common_utils.get_a_name(self.prefix)
        lda_model = ia.LdaModel(name)

        res = lda_model.train(self.lda_frame, 'paper', 'word', 'count',
                              num_topics=5, max_iterations=150, random_seed=5, alpha=-1.0)

        self._confirm_model_valid(res)

    def test_lda_model_int32_count(self):
        """Standard test for the LDA Model code using int32 count."""
        name = common_utils.get_a_name(self.prefix)
        lda_model = ia.LdaModel(name)

        # Add a new column to change column type from int64 to int32.
        self.lda_frame.add_columns(lambda row: row.count,
                                   ('count32', ia.int32))

        res = lda_model.train(self.lda_frame, 'paper', 'word', 'count32',
                              num_topics=5, max_iterations=150, random_seed=5, alpha=-1.0)

        self._confirm_model_valid(res)

    def test_lda_predict(self):
        """Standard test for the LDA Model predict."""
        name = common_utils.get_a_name(self.prefix)
        lda_model = ia.LdaModel(name)

        # Add a new column to change column type from int64 to int32.
        self.lda_frame.add_columns(lambda row: row.count,
                                   ('count32', ia.int32))

        res = lda_model.train(self.lda_frame, 'paper', 'word', 'count32',
                              num_topics=5, max_iterations=120, random_seed=5, alpha=-1.0)

        predict_vals1 = lda_model.predict(
            "word-0-0,word-1-0,word-2-0,word-3-0,word-4-0")["topics_given_doc"]
        predict_vals2 = lda_model.predict(
            "word-5-0,word-6-0,word-7-0,word-8-0,word-9-0")["topics_given_doc"]
        for i in zip(predict_vals1, predict_vals2):
            self.assertAlmostEqual(predict_vals1, predict_vals2)

    def _confirm_model_valid(self, results_dict):
        """Validates the results of lda train."""
        # fetch all the words, and all the docs
        report = results_dict['report']
        print report
        self.assertIn("maxIterations", report)

        print results_dict

        word_topic_res = results_dict['word_given_topics']
        topic_words_res = results_dict['topics_given_word']
        topic_doc_res = results_dict['topics_given_doc']
        print word_topic_res.inspect()
        print topic_words_res.inspect()
        print topic_doc_res.inspect()
        words = word_topic_res.take(word_topic_res.row_count)
        topics = topic_words_res.take(topic_words_res.row_count)
        docs = topic_doc_res.take(topic_doc_res.row_count)

        baseline_words = filter(lambda x: x[0][-1] != '-', words)
        base_topics = filter(lambda x: x[0][-1] != '-', topics)

        # get the baseline values
        all_rows = self.lda_frame.take(self.lda_frame.row_count)

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
            print res
            self.assertGreater(max(res.values())/total, .9)
        # check the grouped baseline words all have the same index val maxed
        # in their associated word
        for i in words_grouped:
            grouped_words = filter(lambda a: a[0] in i, baseline_words)
            print grouped_words
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
            positions = map(lambda x: max_index(map(float, x[1])), grouped_words)
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
        name = common_utils.get_a_name(self.prefix)
        lda_model = ia.LdaModel(name)

        with(self.assertRaises(ia.rest.command.CommandServerError)):
            lda_model.train(None, 'paper', 'word', 'count',
                            num_topics=5)

    def test_lda_column_bad_type(self):
        """Test passing bad type to docs column."""
        name = common_utils.get_a_name(self.prefix)
        lda_model = ia.LdaModel(name)

        with(self.assertRaises(ia.rest.command.CommandServerError)):
            lda_model.train(self.lda_frame, 5, 'word', 'count',
                            num_topics=5)

    def test_lda_max_iterations_negative(self):
        """Test max_iterations with a negative value."""
        name = common_utils.get_a_name(self.prefix)
        lda_model = ia.LdaModel(name)

        with(self.assertRaises(ia.rest.command.CommandServerError)):
            lda_model.train(self.lda_frame, 'paper', 'word', 'count',
                            num_topics=5, max_iterations=-6)

    def test_lda_negative_topics(self):
        """Test max_iterations with a negative value."""
        name = common_utils.get_a_name(self.prefix)
        lda_model = ia.LdaModel(name)

        with(self.assertRaises(ia.rest.command.CommandServerError)):
            lda_model.train(self.lda_frame, 'paper', 'word', 'count',
                            num_topics=-5)

    def test_lda_alpha_negative(self):
        """Test lda with a negative alpha."""
        name = common_utils.get_a_name(self.prefix)
        lda_model = ia.LdaModel(name)

        with(self.assertRaises(ia.rest.command.CommandServerError)):
            lda_model.train(self.lda_frame, 'paper', 'word', 'count',
                            num_topics=5, alpha=-.1)

    def test_lda_seed_type(self):
        """Test lda with a bad seed type."""
        name = common_utils.get_a_name(self.prefix)
        lda_model = ia.LdaModel(name)

        with(self.assertRaises(ia.rest.command.CommandServerError)):
            lda_model.train(self.lda_frame, 'paper', 'word', 'count',
                            num_topics=5, random_seed="foo")

    def test_lda_no_random_seed(self):
        """Test lda with a fixed random seed"""
        name = common_utils.get_a_name(self.prefix)
        lda_model = ia.LdaModel(name)

        lda_model.train(
            self.lda_frame, 'paper', 'word', 'count',
            num_topics=5, random_seed=0, max_iterations=120, alpha=-1.0)

    def test_lda_publish(self):
        """Test training for more iterations."""
        name = common_utils.get_a_name(self.prefix)
        lda_model = ia.LdaModel(name)

        res = lda_model.train(self.lda_frame, 'paper', 'word', 'count',
                              num_topics=5, max_iterations=130, random_seed=5)
        path = lda_model.publish()
        self.assertIn("hdfs", path)
        self.assertIn("tar", path)

    def test_lda_max_iterations(self):
        """Test training for more iterations."""
        name = common_utils.get_a_name(self.prefix)
        lda_model = ia.LdaModel(name)

        res = lda_model.train(self.lda_frame, 'paper', 'word', 'count',
                              num_topics=5, max_iterations=130, random_seed=5, alpha=-1.0)

        self._confirm_model_valid(res)

    def test_lda_alpha_beta_large(self):
        """Test alpha and beta behave correctly."""
        name = common_utils.get_a_name(self.prefix)
        lda_model = ia.LdaModel(name)

        res = lda_model.train(self.lda_frame, 'paper', 'word', 'count',
                              num_topics=5, alpha=10000000.0, beta=10000000.0,
                              max_iterations=90)

        print res
        self._confirm_model_valid(res)
        # test the lists are completely uniform (as expected)

        word_report = res['word_given_topics']
        words = word_report.take(word_report.row_count)

        doc_results = res['topics_given_doc']
        docs = doc_results.take(word_report.row_count)

        topic_results = res['topics_given_word']
        topics = topic_results.take(word_report.row_count)

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
