"""
   Support file for Remote UDF testing.
   Called indirectly from test cases, going through functions in
     udf_remote_utils_direct.py
"""

__author__ = "WDW"
__credits__ = ["Prune Wickart", "Anjali Sood"]
__version__ = "2015.01.12"


import math


# Compute diagonal distance, given orthogonal sides
def distance(row):
    return math.sqrt(float(row["num1"]) ** 2 + float(row["num2"]) ** 2)


# Select value from indicated column
def selector(row):
    if row["letter"] == 'b':
        return row["num2"]
    else:
        return row["num1"]


def length(mystr):
    """
    Trivial utility from Ajnali's original test case
    :param mystr: input string
    :return: int    length of string
    """
    return len(mystr)
