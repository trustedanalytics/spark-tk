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
from sparktkregtests.lib import sparktk_test
from pyspark import SparkContext
from pyspark.mllib import classification
from pyspark.mllib.regression import LabeledPoint


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

    def test_model_train_empty_feature(self):
        """Test empty string for training features throws errors."""
        with self.assertRaisesRegexp(Exception,
                                     "observationColumn must not be null nor empty"):
            self.context.models.classification.naive_bayes.train(self.frame,
                                                                 "label",
                                                                 "")

    def test_model_train_empty_label_coloum(self):
        """Test empty string for label coloum throws error."""
        with self.assertRaisesRegexp(Exception,
                                     "labelColumn must not be null nor empty"):
            self.context.models.classification.naive_bayes.train(self.frame,
                                                                 "",
                                                                 "['f1', 'f2', 'f3']")

    def test_model_test(self):
        """Test training intializes theta, pi and labels"""
        model = self.context.models.classification.naive_bayes.train(self.frame,
                                                                     "label",
                                                                     ['f1', 'f2', 'f3'])

        res = model.test(self.frame)
        true_pos = float(res.confusion_matrix["Predicted_Pos"]["Actual_Pos"])
        false_neg = float(res.confusion_matrix["Predicted_Neg"]["Actual_Pos"])
        false_pos = float(res.confusion_matrix["Predicted_Pos"]["Actual_Neg"])
        true_neg = float(res.confusion_matrix["Predicted_Neg"]["Actual_Neg"])
        recall = true_pos / (false_neg + true_pos)
        precision = true_pos / (false_pos + true_pos)
        f_measure = float(2) / (float(1/precision) + float(1/recall))
        accuracy = float(true_pos + true_neg) / self.frame.count()
        self.assertAlmostEqual(res.recall, recall)
        self.assertAlmostEqual(res.precision, precision)
        self.assertAlmostEqual(res.f_measure, f_measure)
        self.assertAlmostEqual(res.accuracy, accuracy)

    @unittest.skip("model publish does not yet exist in dev")
    def test_model_publish_bayes(self):
        """Test training intializes theta, pi and labels"""
        model = self.context.models.classification.naive_bayes.train(self.frame,
                                                                     "label",
                                                                     ['f1', 'f2', 'f3'])
        path = model.publish()
        self.assertIn("hdfs", path)
        self.assertIn("tar", path)

    def test_model_test_paramater_initiation(self):
        """Test training intializes theta, pi and labels"""
        # we will compare pyspark's result with sparktks for predict
        # points will be an array of pyspark LabelPoints
        points = []
        # the location of the dataset
        location = self.get_local_dataset("naive_bayes.csv")
        
        # we have to build a dataset for pyspark
        # pyspark expects an rdd of LabelPoints for
        # its NaiveBayes model
        with open(location, 'r') as datafile:
            lines = datafile.read().split('\n')
            dataset = []
            # for each line, split into columns and
            # create a label point object out of each line
            for line in lines:
                if line is not "":
                    line = map(int, line.split(","))
                    label = line[0]
                    features = line[1:4]
                    lp = LabeledPoint(label, features)
                    points.append(lp)
        # use pyspark context to parallelize
        dataframe = self.context.sc.parallelize(points)
        
        # create a pyspark model from the data and a sparktk model
        pyspark_model = classification.NaiveBayes.train(dataframe, 1.0)
        model = self.context.models.classification.naive_bayes.train(self.frame,
                                                                     "label",
                                                                     ['f1', 'f2', 'f3'])

        # use our sparktk model to predict, download to pandas for 
        # ease of comparison
        model.predict(self.frame, ['f1', 'f2', 'f3'])
        analysis = self.frame.to_pandas()
        
        # iterate through the sparktk result and compare the prediction
        # with pyspark's prediction
        for index, row in analysis.iterrows():
            # extract the features
            features = [row["f1"], row["f2"], row["f3"]]
            # use the features to get pyspark's result
            pyspark_result = pyspark_model.predict(features)
            self.assertEqual(row["predicted_class"], pyspark_result)


if __name__ == '__main__':
    unittest.main()
