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
import operator as o

def rand_forest_class(features, ptcnt, filename):
    """Generates data random forest dataset with 2 features"""
    with open(filename, "w") as file_ptr:
        for i in xrange(ptcnt):
           r = [random.randint(-100,100) for i in xrange(features)]
           file_ptr.write(",".join(map(str, r))+"\n")
    
if __name__ == "__main__":
    rand_forest_class(1001, 10**6, "rand_forest_1M.csv")
