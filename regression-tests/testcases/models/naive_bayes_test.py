""" Tests Naive Bayes Model against known values.  """

import unittest
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))
from qalib import sparktk_test


class NaiveBayes(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Build the frames needed for the tests."""
        super(NaiveBayes, self).setUp()

        dataset = self.get_file("naive_bayes_data.txt")
        schema = [("label", int),
                  ("f1", int),
                  ("f2", int),
                  ("f3", int)]
        self.frame = self.context.frame.import_csv(dataset, schema=schema)

    def test_model_train_empty_feature(self):
        """Test empty string for training features throws errors."""
        #model = ia.NaiveBayesModel()
        with self.assertRaises(Exception):
            self.context.models.classification.naive_bayes.train(self.frame, "label", "")

    def test_model_train_empty_label_coloum(self):
        """Test empty string for label coloum throws error."""
        #model = ia.NaiveBayesModel()
        with self.assertRaises(Exception):
            self.context.models.classification.naive_bayes.train(self.frame, "", "['f1', 'f2', 'f3']")

    def test_model_test(self):
        """Test training intializes theta, pi and labels"""
        #model = ia.NaiveBayesModel()
        model = self.context.models.classification.naive_bayes.train(self.frame, "label", ['f1', 'f2', 'f3'])

        # Test is perfrect
        res = model.test(self.frame, "label", ['f1', 'f2', 'f3'])
        self.assertAlmostEqual(res.recall, 1.0)
        self.assertAlmostEqual(res.precision, 1.0)
        self.assertAlmostEqual(res.f_measure, 1.0)
        self.assertAlmostEqual(res.accuracy, 1.0)
        self.assertAlmostEqual(
            res.confusion_matrix["Predicted_Pos"]["Actual_Pos"], 2)
        self.assertAlmostEqual(
            res.confusion_matrix["Predicted_Pos"]["Actual_Neg"], 0)
        self.assertAlmostEqual(
            res.confusion_matrix["Predicted_Neg"]["Actual_Neg"], 4)
        self.assertAlmostEqual(
            res.confusion_matrix["Predicted_Neg"]["Actual_Pos"], 0)

    def test_model_publish_bayes(self):
        """Test training intializes theta, pi and labels"""
        #model = ia.NaiveBayesModel()
        model = self.context.models.classification.naive_bayes.train(self.frame, "label", ['f1', 'f2', 'f3'])
        path = model.publish()
        self.assertIn("hdfs", path)
        self.assertIn("tar", path)

    def test_model_test_paramater_initiation(self):
        """Test training intializes theta, pi and labels"""
        #model = ia.NaiveBayesModel()
        model = self.context.models.classification.naive_bayes.train(self.frame, "label", ['f1', 'f2', 'f3'])

        res = model.predict(self.frame, ['f1', 'f2', 'f3'])
        analysis = res.download()
        for (ix, i) in analysis.iterrows():
            self.assertEqual(i["predicted_class"], i["label"])


if __name__ == '__main__':
    unittest.main()
