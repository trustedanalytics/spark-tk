""" test cases for LDA implementation """
import unittest
from sparktkregtests.lib import sparktk_test
from sparktkregtests.lib import scoring_utils


class LDAModelTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Set LDA frame."""
        super(LDAModelTest, self).setUp()

        schema = [('paper', str),
                  ('word', str),
                  ('count', int),
                  ('topic', str)]
        self.lda_frame = self.context.frame.import_csv(self.get_file("lda8.csv"), schema=schema)

    def test_lda_model_int64_count(self):
        """Standard test for the LDA Model code using int64 count."""

        model = self.context.models.clustering.lda.train(self.lda_frame, 'paper', 'word', 'count',
                              num_topics=5, max_iterations=10, random_seed=0)

        test_phrase = ["word-0-0", "word-1-0",
                       "word-2-0", "word-3-0", "word-4-0"]

        file_name = self.get_name("lda")
        model_path = model.export_to_mar(self.get_export_file(file_name))

        res = lda_model.predict(test_phrase)["topics_given_doc"]

        with scoring_utils.scorer(model_path) as scorer:
            result = scorer.score([{"paper":test_phrase}]).json()
            for i, j in zip(res, result[u"data"][0]["topics_given_doc"]):
                self.assertAlmostEqual(i, j)


if __name__ == '__main__':
    unittest.main()
