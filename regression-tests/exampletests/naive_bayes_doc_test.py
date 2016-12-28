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

""" test cases for the naive bayes algorithm documentation script
    usage: python2.7 naive_bayes_doc_test.py
"""

import itertools
import unittest
import random
from sparktk import TkContext


class ClassifierTest(unittest.TestCase):

    def test_model_class_doc(self):
        """Generate a naive bayes dataset, use sparktk to train a model and verify"""
        # Naive bayes is a machine learning algorithm
        # We can use it to classify some item with properties into a group probabilistically
        # The general work flow is to generate a dataset
        # then we calculate the coefficient table and probabilities
        # Finally we build a frame of the data and create a naive bayes model
        # then we test the result of the naive bayes model test

        # Generate naive bayes dataset
        numCoeffs = random.randint(2, 10)
        coefficients = []
        schema = []
        obsCols = []
        dataRows = []
        coeffTable = []
        # the number of rows of data we will generate
        numDiceRolls = random.randint(3, 30)

        # Generate the coefficient table and schema
        for index in range(0, numCoeffs):
            coefficients.append(random.uniform(0, 1))
            schema.append(("x" + str(index), int))
            obsCols.append("x" + str(index))
        schema.append(("x" + str(numCoeffs), int))

        # get all permutations of 0, 1 of length numCoeffs
        binaryPermutations = list(
            itertools.product(range(2), repeat=numCoeffs))

        # now we compute the probability for each row
        # and add the probability for each row as a column to the table
        for element in binaryPermutations:
            product = 1
            element = list(element)
            for i in range(0, numCoeffs):
                if element[i] is 1:
                    product = coefficients[i] * product
                if element[i] is 0:
                    product = (1 - coefficients[i]) * product
            element.append(product)
            coeffTable.append(list(element))

        # Now we use the coefficient table to geneate the actual data
        for row in coeffTable:
            probability = row[len(row) - 1]
            for n in range(0, numDiceRolls):
                newRow = row
                randomResult = random.uniform(0, 1)
            if probability >= randomResult:
                newRow[len(newRow) - 1] = 1
            else:
                newRow[len(newRow) - 1] = 0
            dataRows.append(newRow)

        # Finally we create the frame and model
        # and check that it performs as we would expect
        # We create a sparktk context
        context = TkContext()
        # Then we create a frame from the data
        frame = context.frame.create(dataRows, schema=schema)
        # we train a naive bayes model
        # we give the model lots of information on both data and outcomes
        # in this way it learns which outcome to expect from data
        nb_model = context.models.classification.naive_bayes.train(
            frame, obsCols, "x" + str(numCoeffs - 1))
        # then we test the model
        # meaning we try to see how it behaves in predicting outcomes
        # from data that it has been trained to recognize patterns in
        predicted_frame = nb_model.predict(frame)
        result = nb_model.test(predicted_frame)

        # Lastly we check the result of the model test
        self.assertAlmostEqual(1, result.precision)
        self.assertAlmostEqual(1, result.accuracy)
        self.assertAlmostEqual(1, result.recall)
        self.assertAlmostEqual(1, result.f_measure)


if __name__ == '__main__':
    unittest.main()
