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

# vim: set encoding=utf-8

#  Copyright (c) 2016 Intel Corporation 
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

""" Tests Logistic Regression scoring engine """
import unittest
import os
from sparktkregtests.lib import sparktk_test
from sparktkregtests.lib import scoring_utils
from ConfigParser import SafeConfigParser


class LogisticRegression(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Build test frame"""
        super(LogisticRegression, self).setUp()
        binomial_dataset = self.get_file("small_logit_binary.csv")
        schema = [("vec0", float),
                  ("vec1", float),
                  ("vec2", float),
                  ("vec3", float),
                  ("vec4", float),
                  ("res", int),
                  ("count", int),
                  ("actual", int)]

        self.frame = self.context.frame.import_csv(
            binomial_dataset, schema=schema, header=True)
        self.config = SafeConfigParser()
        filepath = os.path.abspath(os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "..", "..", "lib", "port.ini"))
        self.config.read(filepath)

    def test_model_scoring(self):
        """Test publishing a logistic regression model"""
        model = self.context.models.classification.logistic_regression.train(
            self.frame, ["vec0", "vec1", "vec2", "vec3", "vec4"],
            'res')

        predict = model.predict(
            self.frame,
            ["vec0", "vec1", "vec2", "vec3", "vec4"])
        test_rows = predict.to_pandas(100)

        file_name = self.get_name("logistic_regression")
        model_path = model.export_to_mar(self.get_export_file(file_name))

        with scoring_utils.scorer(
                model_path, self.config.get('port', self.id())) as scorer:
            for i, row in test_rows.iterrows():
                res = scorer.score(
                    [dict(zip(["vec0", "vec1", "vec2", "vec3", "vec4"], list(row[0:5])))])

                self.assertEqual(
                    row["predicted_label"], res.json()["data"][0]['PredictedLabel'])

if __name__ == '__main__':
    unittest.main()
