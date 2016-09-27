
import random
import math
import numpy

def data_generate(users, items):
    user_factors = numpy.array([[random.random()*5 for i in range(3)] for i in range(users)])
    item_factors = numpy.array([[random.random()*5 for j in range(items)] for i in range(3)])

    values = numpy.dot(user_factors, item_factors)
    for u in range(users):
        for i in range(items):
            print ",".join(["user-"+str(u), "item-"+str(i), str(values[int(u)][int(i)])])




if __name__ == "__main__":
    data_generate(40, 40)
