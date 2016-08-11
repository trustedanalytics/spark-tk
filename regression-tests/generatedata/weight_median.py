with open("../datasets/weight_median.csv", 'w') as f:
    for i in xrange(0, 1000):
        f.write(str(i) + "," + str(i) + "\n")
