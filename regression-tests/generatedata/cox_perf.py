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

def perf_val(count, width, filename):
    widthcount = range(width)
    
    with open(filename, 'w') as filewr:
        for i in xrange(count):
            filewr.write(",".join(map(str, ["1", i] + [random.uniform(-100, 100) for j in widthcount])) + '\n')


if __name__ == "__main__":
    perf_val(10 ** 7, 100, "cox_perf10M_wide.csv")
