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

import random
import itertools


def generate_data_set(listOfCoeffs, numDiceRolls):
    """generatre a naive bayes dataset"""
    # numDiceRolls denotes the number of times to generate
    # a data row for each probability, e.g., if the numDiceRolls
    # is 100, for each probability in the probability table
    # we will generate 100 rows of data, so the number of
    # data rows will be the number of probabilities * numDiceRolls
    # the number of rows should be 2 ^ count(listOfCoeffs)
    numCoeffs = len(listOfCoeffs)
    dataRows = ""
    coeffTable = generate_naive_bayes_table(listOfCoeffs, numCoeffs)
    dataRows = generate_random_data_from_probability_table(coeffTable,
                                                           dataRows,
                                                           numCoeffs,
                                                           numDiceRolls)
    with open("../datasets/naive_bayes.csv", "w") as file:
        file.write(dataRows)


def generate_random_data_from_probability_table(coeffTable, dataRows, numCoeffs, numDiceRolls):
    """given a probability table, generate data from it"""
    for row in coeffTable:
        probability = row[len(row) - 1]
        for n in range(0, numDiceRolls):
            newRow = row
            newRow[len(newRow) - 1] = roll_dice(probability)
            rowLine = str(newRow)
            rowLine = rowLine.replace("[", "")
            rowLine = rowLine.replace("]", "")
            rowLine = rowLine.replace(" ", "")
            dataRows = dataRows + rowLine + "\n"
    return dataRows


def generate_naive_bayes_table(listOfCoeffs, numCoeffs):
    """compute the coefficient table for naive bayes dataset"""
    # gets all permutations of 0 and 1 of length numCoeffs
    binaryPermutations = list(itertools.product(range(2), repeat=numCoeffs))
    coeffTable = []
    # now we compute the prob for each row and add the prob for
    # each row as a col to the table
    for element in binaryPermutations:
        product = 1
        element = list(element)
        for i in range(0, numCoeffs):
            if element[i] is 1:
                product = listOfCoeffs[i] * product
            if element[i] is 0:
                product = (1 - listOfCoeffs[i]) * product
        element.append(product)
        coeffTable.append(list(element))
    return coeffTable


def roll_dice(probability):
    """given a probability, generate 1 or 0"""
    randomResult = random.uniform(0, 1)
    if probability >= randomResult:
        return 1
    else:
        return 0


if __name__ == "__main__":
    generate_data_set([0.3, 0.4, 0.3], 500)
