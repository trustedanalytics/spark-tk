import unittest
from sparktkregtests.lib import sparktk_test
from sparktkregtests.lib import scoring_utils

class PrincipalComponent(sparktk_test.SparkTKTestCase):

    def setUp(self):
        super(PrincipalComponent, self).setUp()
        schema = [("X1", int),
                  ("X2", int),
                  ("X3", int),
                  ("X4", int),
                  ("X5", int),
                  ("X6", int),
                  ("X7", int),
                  ("X8", int),
                  ("X9", int),
                  ("X10", int)]
        pca_traindata = self.get_file("pcadata.csv")
        self.frame = self.context.frame.import_csv(pca_traindata, schema=schema)

    def test_pca_train(self):
        """Test the train functionality and basic scoring"""
        model = self.context.models.dimreduction.pca.train(
            self.frame,
            ["X1", "X2", "X3", "X4", "X5",
            "X6", "X7", "X8", "X9", "X10"],
            False, 10)

        file_name = self.get_name("pca")
        model_path = model.export_to_mar(self.get_export_file(file_name))

        with scoring_utils.scorer(model_path) as scorer:
            baseline = model.predict(self.frame, mean_centered=False)
            testvals = baseline.to_pandas(50)

            for _, i in testvals.iterrows():
                r = scorer.score(
                    [dict(zip(["X1", "X2", "X3", "X4", "X5",
                               "X6", "X7", "X8", "X9", "X10"],
                    map(lambda x: x, i[0:10])))])
                map(lambda x, y: self.assertAlmostEqual(float(x),float(y)),
                    r.json()["data"][-1]["principal_components"], i[10:])


if __name__ == '__main__':
    unittest.main()
