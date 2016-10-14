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


class KMeansClustering(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Import the files to test against."""
        super(KMeansClustering, self).setUp()
        schema = [("Vec1", float),
                  ("Vec2", float),
                  ("Vec3", float),
                  ("Vec4", float),
                  ("Vec5", float),
                  ("term", str)]

        self.frame_train = self.context.frame.import_csv(
            self.get_file("kmeans_5c_5d_5000pt_train.csv"), schema=schema)
        self.frame_test = self.context.frame.import_csv(
            self.get_file("kmeans_5c_5d_5000pt_test.csv"), schema=schema)

    def test_kmeans_standard(self):
        """Tests standard usage of the kmeans cluster algorithm."""
        # No asserts because this test is too unstable
        kmodel = self.context.models.clustering.kmeans.train(
            self.frame_train, ["Vec1", "Vec2", "Vec3", "Vec4", "Vec5"], 5)

        result_frame = kmodel.predict(self.frame_test)
        test_rows = result_frame.to_pandas(50)
        result = kmodel.export_to_mar(self.get_export_file(self.get_name("kmeans")))

        with scoring_utils.scorer(result) as scorer:
            for _, i in test_rows.iterrows():
                res = scorer.score(
                    [dict(zip(["Vec1", "Vec2", "Vec3", "Vec4", "Vec5"],
                    list(i[0:5])))])

                self.assertEqual(i["cluster"], res.json()["data"][0]['score'])


if __name__ == '__main__':
    unittest.main()
