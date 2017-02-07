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


""" Tests scoring pipeline with naive bayes model  """
import unittest
import os
from sparktkregtests.lib import sparktk_test
from sparktkregtests.lib import scoring_utils
from sparktkregtests.lib import config
import subprocess
import tarfile


class PipelineNaiveBayes(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Build the frames needed for the tests."""
        super(PipelineNaiveBayes, self).setUp()

        dataset = self.get_file("naive_bayes.csv")
        schema = [("label", str),
                  ("f1", str),
                  ("f2", str),
                  ("f3", str)]
        self.frame = self.context.frame.import_csv(dataset, schema=schema)

    def test_scoring_pipeline(self):
        """Test scoring_pipeline"""
        model = self.context.models.classification.naive_bayes.train(
            self.frame, ['f1', 'f2', 'f3'], "label")
        res = model.predict(self.frame, ['f1', 'f2', 'f3'])
        analysis = res.to_pandas()
        file_name = self.get_name("naive_bayes")
        model_path = model.export_to_mar(self.get_export_file(file_name))

        self.tarfile = "pipeline.tar"
        pipeline_funcs = os.path.join(
            config.root, "regression-tests", "sparktkregtests",
            "testcases", "scoretests", "pipeline_funcs.py")
        pipeline_config = os.path.join(
            config.root, "regression-tests", "sparktkregtests",
            "testcases", "scoretests", "pipeline_config.json")

        tar = tarfile.open(self.tarfile, "w:gz")
        tar.add(pipeline_funcs, "pipeline_funcs.py")
        tar.add(pipeline_config, "pipeline_config.json")
        tar.close()

        with scoring_utils.scorer(
                model_path, self.id(), pipeline=True,
                pipeline_filename=self.tarfile) as scorer:
            for _, i in analysis.iterrows():
                r = scorer.score(
                    [dict(zip(['f1', 'f2', 'f3'],
                          map(lambda x: int(x), (i[1:4]))))])
                self.assertEqual(
                    r.json(), i['predicted_class'])

    def tearDown(self):
        super(PipelineNaiveBayes, self).tearDown()
        subprocess.call(['rm', self.tarfile])


if __name__ == '__main__':
    unittest.main()
