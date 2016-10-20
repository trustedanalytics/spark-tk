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

""" Tests Linear Regression scoring engine """
import unittest

from sparktkregtests.lib import sparktk_test
from sparktkregtests.lib import scoring_utils


class ArimaxScoretest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        super(ArimaxScoretest, self).setUp()
        schema = [("Date", str),
                  ("Time", str),
                  ("COGT", float),
                  ("PT08S1CO", float),
                  ("NMHCGT", float),
                  ("C6H6GT", float),
                  ("PT08S2NMHC", float),
                  ("NOxGT", float),
                  ("PT08S3NOx", float),
                  ("NO2GT", float),
                  ("PT08S4NO2", float),
                  ("PT08S5O3", float),
                  ("T", float),
                  ("RH", float),
                  ("AH", float)]

        dataset = self.get_file("AirQualityUCI.csv")
        self.frame = self.context.frame.import_csv(dataset, schema=schema, delimiter=";", header=True)
        self.train_frame = self.frame.copy(where= lambda row: row.Date != "04/04/2005")
        self.actual_data = self.frame.copy(where= lambda row: row.Date == "04/04/2005") #last 15 rows
        self.ts_column = "COGT"
        self.x_columns = ["PT08S2NMHC", "NOxGT", "PT08S3NOx", "NO2GT", "PT08S4NO2", "PT08S5O3"]

    def test_arimax_scoring(self):
        """Test arx predict method"""
        model = self.context.models.timeseries.arx.train(self.frame, self.ts_column, self.x_columns, 0, 0, False)

        predict = model.predict(self.actual_data, self.ts_column, self.x_columns)
        test_rows = predict.to_pandas(predict.count())
        model_path = model.export_to_mar(self.get_export_file("ArimaX"))
        with scoring_utils.scorer(model_path) as scorer:
            for index, row in test_rows.iterrows():
                record = { "future" : float(row[2]), "timeseries" : list(row[6:12]) }
                print "record: " + str(record)
                res = scorer.score(
                    [record])
                print "result: " + str(res)
                self.assertEqual(
                    row["predicted_y"], res.json()["data"][0]['Prediction'])


if __name__ == '__main__':
    unittest.main()
