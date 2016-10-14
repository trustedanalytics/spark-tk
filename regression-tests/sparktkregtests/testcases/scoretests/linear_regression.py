""" Tests Linear Regression scoring engine """
import unittest

from sparktkregtests.lib import sparktk_test
from sparktkregtests.lib import scoring_utils


class LinearRegression(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Build test frame"""
        super(LinearRegression, self).setUp()
        dataset = self.get_file("linear_regression_gen.csv")
        schema = [("c1", float),
                  ("c2", float),
                  ("c3", float),
                  ("c4", float),
                  ("label", float)]

        self.frame = self.context.frame.import_csv(
            dataset, schema=schema)

    def test_model_publish(self):
        """Test publishing a linear regression model"""
        model = self.context.models.regression.linear_regression.train(self.frame, "label", ['c1', 'c2', 'c3', 'c4'])

        predict = model.predict(self.frame, ['c1', 'c2', 'c3', 'c4'])
        test_rows = predict.to_pandas(predict.count())

        model_path = model.export_to_mar(self.get_export_file("LinearRegression"))
        with scoring_utils.scorer(model_path) as scorer:
            for _, i in test_rows.iterrows():
                res = scorer.score(
                    [dict(zip(["c1", "c2", "c3", "c4"], list(i[0:4])))])
                self.assertEqual(
                    i["predicted_value"], res.json()["data"][0]['Prediction'])

            


if __name__ == '__main__':
    unittest.main()
