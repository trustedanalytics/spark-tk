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

def main():
    # dataset :: [[([(Int,Int)],string)]]
    # flattened1 :: [([(Int,Int)],String)]
    trainf = open("filetrain_kmeansbill.csv", 'w')
    testf = open("filetest_kmeansbill.csv", 'w')
    resvals = open("truth_bill.txt", 'w')

    dataset = dataSetGenSuperStream(5, 50, 2*10**7, trainf, testf, resvals)

    trainf.close()
    testf.close()
    resvals.close()

# Dataset generator, not used as part of the automated test
# This was used to crete the initial datasets
# This is used for performance tests, it streams the data out
def dataSetGenSuperStream(nodec, dimc, ptc, train, test, res, dist=(lambda: random.uniform(-1.0, 1.0)), max=1, min=-1):
    # nodec:    quantity of classes (KMeans groupings)
    # dimc:     dimension; quantity of features (frame columns)
    # ptc:      desired rows of output
    # train:    name of training file (output)
    # test:     name of test file (output)
    # res:      name of results file (output)
    # dist:     distribution function for actual mean points
    # max, min: range limits for generated variations

    # vectore = [Int] -> string -> ([Int],String)
    vector = lambda cent : ([(val, dist()) for val in cent])
    # nodes :: [([Int],String)]
    #nodes = [([100*random.uniform(min, max) for _ in range(dimc)], i) for i in range(nodec)]
    nodes = [([0,0,0,0,0],'0'),
             ([10,10,10,10,10],'1'),
             ([-10,-10,-10,-20,10],'2'),
             ([-50,70,-30,90,20],'3'),
             ([60,-70,-40,30,600],'4')]

    flag = 0
    sumsquaredval = 0
    # val :: [[([(Int,Int)],String)]]
    for (cent, name) in nodes:
        print name
        res.write(','.join(map(str,cent)) + ",Vec" + str(name) + '\n')
        for j in range(ptc/nodec):
            i = vector(cent)

            sum = reduce(lambda agg, x: agg+x*x, map(lambda (x,y): y, i), 0)
            sumsquaredval = sumsquaredval + sum
            vals = map(lambda (x,y): str(x+y), i)
            to_print = ','.join(vals + ["Vec" + str(name)])

            if flag % 2 == 1:
                train.write(to_print+'\n')
            else:
                test.write(to_print+'\n')
            flag = flag + 1    
    res.write(str(sumsquaredval) + '\n')

# Dataset generator, not used as part of the automated test
# This was used to crete the initial datasets
# This is used for performance tests, it streams the data out
def dataSetGenStream(nodec, dimc, ptc, train, test, dist=(lambda : random.uniform(-10,10)), max=100, min=0):
    # vectore = [Int] -> string -> ([Int],String)
    vector = lambda cent, name : (','.join([str(val+dist()) for val in cent])+','+name)
    # nodes :: [([Int],String)]
    nodes = [([random.uniform(min, max) for _ in range(dimc)], i) for i in range(nodec)]
    flag = 0
    # val :: [[([(Int,Int)],String)]]
    for (cent, name) in nodes:
        print name
        for i in [vector(cent, "Vec"+str(name)) for _ in range(ptc/nodec)]:
            if flag % 2 == 1:
                train.write(i+'\n')
            else:
                test.write(i+'\n')
            flag = flag + 1    

# Dataset generator, not used as part of the automated test
# This was used to crete the initial datasets
def dataSetGen(nodec, dimc, ptc, dist=(lambda : random.uniform(-10,10)), max=100, min=0):
    # vectore = [Int] -> string -> ([(Int,Int],String)
    vector = lambda cent, name : ([(val,dist()) for val in cent],name)
    # nodes :: [([Int],String)]
    nodes = [([random.uniform(min, max) for _ in range(dimc)], i) for i in range(nodec)]
    # val :: [[([(Int,Int)],String)]]
    return [[vector(cent, "Vec"+str(name)) for _ in range(ptc/nodec)] for (cent, name) in nodes]

# [[[string]]]
# map (\x -> map \y -> 
def flattenGenData(list3):
    return reduce(lambda s, l2: s+(map(lambda l1: ",".join(map(str, l1)), l2)), list3, [])
                
if __name__ == '__main__':
    main()
