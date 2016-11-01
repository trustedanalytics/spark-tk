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


"""svm model test for scoring"""
import unittest
import os
from sparktkregtests.lib import scoring_utils
from sparktkregtests.lib import sparktk_test


class SvmScoreTest(sparktk_test.SparkTKTestCase):

    def lattice2frame(self, matrix):
        """Convert 2D string lattice to data frame."""
        # The input matrix is a string lattice with data points marked
        #   with + and - (2-class model), or with integers (multi-class).
        #   Any other characters are ignored.
        # The lattice's center is taken as the origin.
        #
        # return: Frame with the positions and values converted to
        #   SVM input requirements.
        # This frame is ready as input to train, test, or predict.

        block_data = []
        schema = [('x', float),
                  ('y', float),
                  ('model_class', int)]

        # Grabbing center column from center row allows for a skew matrix,
        #   so long as the center row is complete.
        origin_y = len(matrix)/2
        origin_x = len(matrix[origin_y])/2

        # print "L2M TRACE", matrix, "origin at", origin_x, origin_y
        for y in range(len(matrix)):
            for x in range(len(matrix[y])):
                svm_class = None
                char = matrix[y][x]
                if char == '+':
                    svm_class = 1
                elif char == '-':
                    svm_class = 0
                elif char.isdigit():
                    svm_class = int(char)
                if svm_class is not None:
                    block_data.append([x-origin_x, origin_y-y, svm_class])
        block_data.sort()

        if len(block_data) == 0:
            frame = None
        else:
            frame = self.context.frame.create(block_data, schema=schema)
        return frame

    def test_model_scoring(self):
        """ Verify that SvmModel operates as expected.  """
        # Test set is a 3x3 square lattice of points
        #   with a fully accurate, linear, unbiased divider.

        train_lattice = ["+++",
                         "++-",
                         "---"]

        training_frame = self.lattice2frame(train_lattice)
        svm_model = self.context.models.classification.svm.train(
            training_frame, u"model_class", ["x", "y"])

        file_name = self.get_name("svm")
        model_path = svm_model.export_to_mar(self.get_export_file(file_name))

        test_rows = training_frame.to_pandas(training_frame.count())
        
        with scoring_utils.scorer(
                model_path, self.id()) as scorer:
            for _, i in test_rows.iterrows():
                res = scorer.score([dict(zip(["x", "y"], list(i[0:2])))])
                self.assertEqual(i[2], res.json()["data"][0]['Prediction'])


if __name__ == "__main__":
    unittest.main()
