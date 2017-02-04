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

import math
import random

#The intercept and coefficients which the ARx model will try to calculate
#If the coefficient values are not in the range (-1,1), the ARx model will not converge
#These values are hardcoded into the tests, so if you change them they need to be changed in the test as well
intercept = 1.335
ar1 = 0.542
ar2 = 0.237
exo1 = 0.1293
exo2 = 0.0781
exo3 = -0.04275

#Initial values for previous two terms in the sequence
#Can not use '-' in variable name, so 'n1' actually means 'n-1'
y_n1 = 1
y_n2 = 1

#Initial values for previous two terms, when calcuate with error term
y_n1_with_err = 1
y_n2_with_err = 1

number_of_rows = 500000

for i in xrange(number_of_rows):
    #Generate exogonous variables
    x1 = random.uniform(-1.0, 1.0)
    x2 = random.uniform(-1.0, 1.0)
    x3 = random.uniform(-1.0, 1.0)
    exo_terms = exo1*x1 + exo2*x2 + exo3*x3

    #Calculate y_n without noise
    ar_terms = ar1*y_n1 + ar2*y_n2
    y_n = ar_terms + exo_terms + intercept

    #Calculate y_n with noise
    ar_terms_with_err = ar1*y_n1_with_err + ar2*y_n2_with_err
    err = random.uniform(-0.1, 0.1)
    y_n_with_err = ar_terms_with_err + exo_terms + intercept + err

    row = [str(i), str(y_n), str(y_n_with_err), str(x1), str(x2), str(x3)]
    print(",".join(row))

    if y_n == float("inf") or y_n_with_err == float("inf"):
        raise RuntimeError('"inf" can not be imported into the dataframe correctly. Try using coefficients between -1 and 1.')

    y_n2 = y_n1
    y_n1 = y_n

    y_n2_with_err = y_n1_with_err
    y_n1_with_err = y_n_with_err
