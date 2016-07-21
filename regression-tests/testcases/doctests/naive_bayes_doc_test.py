""" test cases for the naive bayes algorithm documentation script
    usage: python2.7 naive_bayes_doc_test.py
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
        """Generate a naive bayes dataset, use sparktk to train a model and check the result for accuracy, precision, etc."""	
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
    	self.assertAlmostEqual(1, result.precision)
    	self.assertAlmostEqual(1, result.accuracy)
    	self.assertAlmostEqual(1, result.recall)
    	self.assertAlmostEqual(1, result.f_measure)
if __name__ == '__main__':
    unittest.main()
