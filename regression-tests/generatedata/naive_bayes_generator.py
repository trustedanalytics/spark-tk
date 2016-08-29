import random
import itertools


# list of coefficients is an array of the coefficients
# numDiceRolls is the number of times the code will generate
# a line *for each permutation/possibility* (i.e., each line in the probability table)
def generate_data_set(listOfCoeffs, numDiceRolls):
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


# for each line in the probability table, generate numDiceRolls lines of data
def generate_random_data_from_probability_table(coeffTable, dataRows, numCoeffs, numDiceRolls):
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


# generate the probability table
def generate_naive_bayes_table(listOfCoeffs, numCoeffs):
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


# generate a 1 or 0 based off of the
# given probability (probability of generating a 1)
def roll_dice(probability):
    randomResult = random.uniform(0, 1)
    if probability >= randomResult:
        return 1
    else:
        return 0


if __name__ == "__main__":
    generate_data_set([0.3, 0.4, 0.3], 500)
