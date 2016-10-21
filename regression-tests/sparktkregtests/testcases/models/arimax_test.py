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

"""Tests accuracy of arima, arimax, arx, max predictions against R generated predictions"""

import unittest
from sparktkregtests.lib import sparktk_test
import os
import sys
from sklearn.metrics import mean_absolute_error
from sklearn.metrics import mean_squared_error

class ArimaxTest(sparktk_test.SparkTKTestCase):
    def find_section(self, begin_line, end_line, f):
        """Return all lines between begin_line and end_line in f as a list"""
        data = []
        mode = 0
        for line in f:
            if begin_line in line:
                mode = 1
                continue
            if end_line in line and mode == 1:
                break
            if mode == 1:
                data.append(line.strip('\n'))
        return data

    def parse_arimax(self, model_name):
        """Locate the output of ARIMA, ARx, MAx, or ARIMAx predict function and convert it to a list of floating points"""
        begin_line = "BEGIN " + model_name + " PREDICT"
        end_line = "END " + model_name + " PREDICT"
        with open(self.Routput, 'r') as f:
            data = self.find_section(begin_line, end_line, f)
        p = [element.split() for element in data]
        prediction = [float(val) for sublist in p for val in sublist if '[' not in val]
        return prediction

    def setUp(self):
        super(ArimaxTest, self).setUp()
        self.Routput = self.get_local_dataset('arimaxoutput.txt')
        schema = [("Int", int),
                  ("Date", str),
                  ("Sales", float),
                  ("AdBudget", float),
                  ("GDP", float)]
        dataset = self.get_file("tute1.csv")
        self.frame = self.context.frame.import_csv(dataset, schema=schema, delimiter=",", header=True)
        self.train_frame = self.frame.copy(where= lambda row: row.Int <= 90)
        self.actual_data = self.frame.copy(where= lambda row: row.Int > 90) #last 10 rows
        self.ts_column ="Sales"
        self.x_columns = ["AdBudget", "GDP"]
        expected_data = self.actual_data.take(n=self.actual_data.count(), columns=self.ts_column)
        self.expected_prediction = [item for sublist in expected_data for item in sublist]

    def test_arx_predict(self):
        """Test arx predict method"""
        output = self.context.models.timeseries.arx.train(self.train_frame, self.ts_column, self.x_columns, 0, 0, False)

        predict_frame = output.predict(self.actual_data, self.ts_column, self.x_columns)
        predict_data = predict_frame.take(n=self.actual_data.count(), columns="predicted_y")
        prediction = [item for sublist in predict_data for item in sublist]
        mae = mean_absolute_error(prediction, self.expected_prediction)
        mse = mean_squared_error(prediction, self.expected_prediction)

        r_prediction = self.parse_arimax("ARX")
        r_mae = mean_absolute_error(r_prediction, self.expected_prediction)
        r_mse = mean_squared_error(r_prediction, self.expected_prediction)

        self.assertLess(mse, r_mse+(r_mse/5))
        self.assertLess(mae, r_mae+(r_mae/5))

    def test_arima_predict(self):
        """Test arima train method"""
        timeseries_column = self.train_frame.take(n=self.train_frame.count(), columns=self.ts_column)
        timeseries_data = [item for sublist in timeseries_column for item in sublist]

        output = self.context.models.timeseries.arima.train(timeseries_data, 1, 0, 1)
        predict = output.predict(0)
        prediction = predict[:10]
        mae = mean_absolute_error(prediction, self.expected_prediction)
        mse = mean_squared_error(prediction, self.expected_prediction)

        r_prediction = self.parse_arimax("ARIMA")
        r_mae = mean_absolute_error(r_prediction, self.expected_prediction)
        r_mse = mean_squared_error(r_prediction, self.expected_prediction)

        self.assertLess(mse, r_mse+(r_mse/5))
        self.assertLess(mae, r_mae+(r_mae/5))

if __name__ == "__main__":
    unittest.main()
