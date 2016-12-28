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

"""Validate svm"""

import unittest
from sparktkregtests.lib import sparktk_test


class Svm2DSlope1(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Build the frame needed for these tests"""
        super(Svm2DSlope1, self).setUp()

        sch2 = [("Class", int),            # Class is either 1 or 0.
                ("Dim_1", float),
                ("Dim_2", float)]
        train_file = self.get_file("SVM-2F-train-50X50_1SlopePlus0.csv")
        test_file = self.get_file("SVM-2F-test-50X50_1SlopePlus0.csv")

        self.trainer = self.context.frame.import_csv(train_file,
                                                     schema=sch2)
        self.frame = self.context.frame.import_csv(test_file,
                                                   schema=sch2)

    def test_svm_model_test(self):
        """Test with train and test data generated with same hyperplane"""
        model = self.context.models.classification.svm.train(self.trainer,
                                                             ["Dim_1", "Dim_2"],
                                                             "Class")
        results = model.test(self.frame)

        # assert that model reports acceptable accuracy, etc.
        self.assertEqual(1.0, results.recall)
        self.assertEqual(1.0, results.accuracy)
        self.assertEqual(1.0, results.precision)
        self.assertEqual(1.0, results.f_measure)

        # Now we verify the confusion matrix contains the expected results.
        cf = results.confusion_matrix
        self.assertEqual(cf['Predicted_Pos']['Actual_Pos'], 95)
        self.assertEqual(cf['Predicted_Neg']['Actual_Pos'], 0)
        self.assertEqual(cf['Predicted_Pos']['Actual_Neg'], 0)
        self.assertEqual(cf['Predicted_Neg']['Actual_Neg'], 105)

    def test_svm_model_predict(self):
        """Test the predict function"""
        model = self.context.models.classification.svm.train(self.trainer,
                                                             ["Dim_1", "Dim_2"],
                                                             "Class")
        predicted_frame = model.predict(self.frame)

        outcome = predicted_frame.to_pandas()
        # Verify that values in 'predict' and 'Class' columns match.
        for index, row in outcome.iterrows():
            self.assertEqual(row["Class"], row["predicted_label"])


if __name__ == "__main__":
    unittest.main()
