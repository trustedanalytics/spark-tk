""" test cases for random forest"""
import unittest
from sparktkregtests.lib import sparktk_test
from sparktkregtests.lib import scoring_utils


class RandomForest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Build the required frame"""
        super(RandomForest, self).setUp()

        schema = [("feat1", int), ("feat2", int), ("class", str)]
        filename = self.get_file("rand_forest_class.csv")

        self.frame = self.context.frame.import_csv(filename, schema=schema)

    def test_rand_forest_class_scoring(self):
        """Test binomial classification of random forest model"""
        rfmodel = self.context.models.classification.random_forest_classifier.train(
            self.frame, "class", ["feat1", "feat2"], seed=0)
        
        result_frame = rfmodel.predict(self.frame)
        preddf = result_frame.to_pandas(result_frame.count())

        file_name = self.get_name("random_forest_classifier")
        model_path = rfmodel.export_to_mar(self.get_export_file(file_name))

        with scoring_utils.scorer(model_path) as scorer:
            for i, row in preddf.iterrows():
                res = scorer.score(
                    [dict(zip(["feat1", "feat2"],
                    map(lambda x: x,row[0:2])))])
                self.assertAlmostEqual(
                    float(row[3]), float(res.json()["data"][0]['Prediction']))

    def test_rand_forest_regression(self):
        """Test binomial classification of random forest model"""
        rfmodel = self.context.models.regression.random_forest_regressor.train(
            self.frame, "class", ["feat1", "feat2"], seed=0)

        predresult = rfmodel.predict(self.frame)
        preddf = predresult.to_pandas(predresult.count())

        file_name = self.get_name("random_forest_regressor")
        model_path = rfmodel.export_to_mar(self.get_export_file(file_name))

        with scoring_utils.scorer(model_path) as scorer:
            for i, row in preddf.iterrows():
                res = scorer.score(
                    [dict(zip(["feat1", "feat2"], map(lambda x: x,row[0:2])))])
                self.assertAlmostEqual(
                    float(row[3]), float(res.json()["data"][0]['Prediction']))


if __name__ == '__main__':
    unittest.main()
