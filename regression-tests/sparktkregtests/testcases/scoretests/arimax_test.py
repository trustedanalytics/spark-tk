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

""" test cases for the kmeans clustering algorithm """
import unittest
import time

from sparktkregtests.lib import scoring_utils
from sparktkregtests.lib import sparktk_test

class ArimaxTest(sparktk_test.SparkTKTestCase):
    def setUp(self):
        super(ArimaxTest, self).setUp()
        schema = [("Int", int),
                  ("Date", str),
                  ("Sales", float),
                  ("AdBudget", float),
                  ("GDP", float)]
        dataset = self.get_file("tute1.csv")
        self.frame = self.context.frame.import_csv(dataset, schema=schema, delimiter=",", header=True)
        self.train_frame = self.frame.copy(where= lambda row: row.Int <= 99)
        self.actual_data = self.frame.copy(where= lambda row: row.Int > 99) #last 10 rows
        self.ts_column ="Sales"
        self.x_columns = ["AdBudget", "GDP"]

    def test_arima_standard(self):
        """Tests standard usage of arima."""
        timeseries_column = self.train_frame.take(n=self.train_frame.count(), columns=self.ts_column)
        timeseries_data = [item for sublist in timeseries_column for item in sublist]

        output = self.context.models.timeseries.arima.train(timeseries_data, 1, 0, 1)
        predict = output.predict(0)
        prediction = predict[:99]
        model_path = output.export_to_mar(self.get_export_file(self.get_name("arima")))
        with scoring_utils.scorer(model_path) as scorer:

            r = scorer.score([{"future":0, "timeseries":timeseries_data}])
            scored =r.json()["data"][0]["predicted_values"]
            self.assertEqual(scored, predict)

    def test_arx_standard(self):
        """Tests standard usage of arx."""
        output = self.context.models.timeseries.arx.train(self.train_frame, self.ts_column, self.x_columns, 0, 0, False)

        predict_frame = output.predict(self.actual_data, self.ts_column, self.x_columns)
        timeseries_column = self.actual_data.take(n=self.actual_data.count(), columns=self.ts_column)
        y = [item for sublist in timeseries_column for item in sublist]
        x_columns = self.actual_data.take(n=self.actual_data.count(), columns=self.x_columns)
        x = [item for sublist in x_columns for item in sublist]
        predict_data = predict_frame.take(n=self.actual_data.count(), columns="predicted_y")
        expected_score  =[item for sublist in predict_data for item in sublist]
        model_path = output.export_to_mar(self.get_export_file(self.get_name("arx")))

        with scoring_utils.scorer(model_path) as scorer:
            r = scorer.score([{"y":y,"x_values":x}])
            scored =r.json()["data"][0]["score"]
            self.assertEqual(scored, expected_score)

    def test_arimax_standard(self):
        """Tests standard usage of arimax."""
        output = self.context.models.timeseries.arimax.train(self.train_frame, self.ts_column, self.x_columns, 1, 0, 1, 0)

        predict_frame = output.predict(self.actual_data, self.ts_column, self.x_columns)
        timeseries_column = self.actual_data.take(n=self.actual_data.count(), columns=self.ts_column)
        y = [item for sublist in timeseries_column for item in sublist]
        x_columns = self.actual_data.take(n=self.actual_data.count(), columns=self.x_columns)
        x = [item for sublist in x_columns for item in sublist]
        predict_data = predict_frame.take(n=self.actual_data.count(), columns="predicted_y")
        expected_score = [item for sublist in predict_data for item in sublist]
        model_path = output.export_to_mar(self.get_export_file(self.get_name("arimax")))

        with scoring_utils.scorer(model_path) as scorer:

            r = scorer.score([{"y":y,"x_values":x}])
            scored =r.json()["data"][0]["score"]
            self.assertEqual(scored, expected_score)

    def test_max_standard(self):
        """Tests standard usage of max."""
        output = self.context.models.timeseries.max.train(self.train_frame, self.ts_column, self.x_columns, 1, 0)

        predict_frame = output.predict(self.actual_data, self.ts_column, self.x_columns)
        timeseries_column = self.actual_data.take(n=self.actual_data.count(), columns=self.ts_column)
        y = [item for sublist in timeseries_column for item in sublist]
        x_columns = self.actual_data.take(n=self.actual_data.count(), columns=self.x_columns)
        x = [item for sublist in x_columns for item in sublist]
        predict_data = predict_frame.take(n=self.actual_data.count(), columns="predicted_y")
        expected_score = [item for sublist in predict_data for item in sublist]
        model_path = output.export_to_mar(self.get_export_file(self.get_name("max")))

        with scoring_utils.scorer(model_path) as scorer:

            r = scorer.score([{"y":y,"x_values":x}])
            scored =r.json()["data"][0]["score"]
            self.assertEqual(scored, expected_score)

if __name__ == '__main__':
    unittest.main()
