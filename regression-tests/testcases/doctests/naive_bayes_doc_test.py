##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2014, 2015 Intel Corporation All Rights Reserved.
#
# The source code contained or described herein and all documents related to
# the source code (Material) are owned by Intel Corporation or its suppliers
# or licensors. Title to the Material remains with Intel Corporation or its
# suppliers and licensors. The Material may contain trade secrets and
# proprietary and confidential information of Intel Corporation and its
# suppliers and licensors, and is protected by worldwide copyright and trade
# secret laws and treaty provisions. No part of the Material may be used,
# copied, reproduced, modified, published, uploaded, posted, transmitted,
# distributed, or disclosed in any way without Intel's prior express written
# permission.
#
# No license under any patent, copyright, trade secret or other intellectual
# property right is granted to or conferred upon you by disclosure or
# delivery of the Materials, either expressly, by implication, inducement,
# estoppel or otherwise. Any license under such intellectual property rights
# must be express and approved by Intel in writing.
##############################################################################
""" test cases for the kmeans clustering algorithm documentation script
    usage: python2.7 naive_bayes_doc_test.py

    THIS TEST REQUIRES NO THIRD PARTY APPLICATIONS OTHER THAN THE ATK
    THIS TEST IS TO BE MAINTAINED AS A SMOKE TEST FOR THE ML SYSTEM
"""

import itertools
import unittest
import os
import random
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))
from qalib import sparktk_test

class ClassifierTest(sparktk_test.SparkTKTestCase):

    def test_model_class_doc(self):
		"""Generate a naive bayes dataset, use sparktk to train a naive bayes model and check that the result meets our expectations for accuracy, precision, etc."""	
		# Generate naive bayes dataset
		numCoeffs = random.randint(2, 10)
		coefficients = []
		schema = []
		obsCols = []
		dataRows = []
		for index in range(0, numCoeffs):
			coefficients.append(random.uniform(0, 1))
			schema.append(("x" + str(index), int))
			obsCols.append("x" + str(index))
		schema.append(("x" + str(numCoeffs), int))
		numDiceRolls = random.randint(3, 30) # the number of rows of data we will generate
		# Generate the coefficient table and schema
		binaryPermutations = list(itertools.product(range(2), repeat=numCoeffs))  # get all permutations of 0, 1 of length numCoeffs
		coeffTable = []
		for element in binaryPermutations: # now we compute the probability for each row and add the probability for each row as a column to the table
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
					newRow[len(newRow) -1] = 1
				else:
					newRow[len(newRow) -1] = 0
				dataRows.append(newRow)
		# Finally we create the frame and model and check that it performs as we would expect
		frame = self.context.frame.create(dataRows, schema=schema)	
		nb_model = self.context.models.classification.naive_bayes.train(frame, "x" + str(numCoeffs - 1), obsCols)
		nb_model.predict(frame)
		result = nb_model.test(frame)
		cm = frame.binary_classification_metrics("x" + str(numCoeffs - 1), "predicted_class", 1)
		self.assertAlmostEqual(1, result.precision)
		self.assertAlmostEqual(1, result.accuracy)
		self.assertAlmostEqual(1, result.recall)
		self.assertAlmostEqual(1, result.f_measure)
if __name__ == '__main__':
    unittest.main()
