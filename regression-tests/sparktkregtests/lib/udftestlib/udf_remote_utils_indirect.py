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
