
import random
import math
import numpy

def data_generate(coefficients, offset, sample_count, per_count, num=5,
    prob_err=0):
    with open("res", 'w') as f:
        f.write(','.join(map(lambda x: "vec"+str(x), range(len(coefficients)))+["res"]+["count"]))
        f.write("\n")
            
        for i in range(sample_count):
            # select a point in the n-dimensional space
            values = [random.uniform(-4,4) for _ in coefficients]

            # ccalculate it's value
            logit = sum([y * x for x, y in zip(values, coefficients)]) + offset
            logvalue = 1 / (1 + math.exp(-1 * logit))

            # convert the point location to a string
            n = map(str, values)

            # select the same line a bunch of times with a certain probability
            # of the result
            if random.uniform(0,1) > prob_err:
                appendvalue = [','.join(n+[str(x), str(random.randint(1, num)), str(logvalue)])
                               for x in numpy.random.choice(2,
                                                            per_count,
                                                            p=[logvalue, 1-logvalue])]
            else:
                val = random.uniform(0,1)
                print "info"
                print val
                print logvalue
                appendvalue = [','.join(n+[str(x), str(random.randint(1, num)), str(logvalue)])
                               for x in numpy.random.choice(2, per_count, p=[val, 1-val])]
            writeval = [','.join(n+[x, str(random.randint(1, num)), ('0' if logvalue < .5 else '1')]) for x in appendvalue]
            f.write('\n'.join(writeval)+'\n')


def data_generatemultinomial(coefficients, offset, sample_count, per_count, num=5):
    with open("res", 'w') as f:
        f.write(','.join(map(lambda x: "vec"+str(x), range(len(coefficients[0])))+["res"]+["count"]))
        f.write("\n")
            
        for i in range(sample_count):
            values = [random.uniform(-4,4) for _ in coefficients[0]]
            logit = [sum([y * x for x, y in zip(values, coeffs)]) + offset for coeffs in coefficients]
            logvalues = [1 / (1 + math.exp(-1 * lgs)) for lgs in logit]
            tot = sum(logvalues)
            normvalues = [x/tot for x in logvalues]
            n = map(str, values)
            actual = normvalues.index(max(normvalues))
            f.write(str(normvalues))
            appendvalue = [','.join(n+[str(x), str(random.randint(1, num)), str(actual)])
                           for x in numpy.random.choice(len(normvalues),
                                                        per_count,
                                                        p=normvalues)]
            f.write('\n'.join(appendvalue)+'\n')

if __name__ == "__main__":
    data_generate([0.4, 0.7, 0.9, 0.3, 1.4], 0.9, (10**3), 1, prob_err=.3)
#    data_generatemultinomial([[0.2, 0.2, 0.4, 0.7, 0.3],
#                              [0.3, 0.5, 0.3, 0.1, 0.3],
#                              [0.1, 0.2, 0.1, 0.1, 0.3],
#                              [0.4, 0.1, 0.2, 0.1, 0.1]], 0.9, (10**4), 20)
