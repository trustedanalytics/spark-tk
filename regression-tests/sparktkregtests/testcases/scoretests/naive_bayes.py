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
