import numpy

def main(coeffs, count, var, width):
    points = [[numpy.random.uniform(-width, width) for i in range(len(coeffs))] for i in range(count)]
    vals = [i+[sum([coeff * j for (j,coeff) in zip(i,coeffs)])] for i in points]


    for i in vals:
        print ','.join(map(str, i))


if __name__ == "__main__":
    main([0.5, -0.7, -.24, 0.4], 1000, 0.5, 3)
