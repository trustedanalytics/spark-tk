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

def rand_forest_class(lines, ptcnt):
    feat = len(lines[0])
    for i in range(ptcnt):
       r = [random.randint(0,10) for i in range(feat-1)]
       val = all(map(lambda ls: sum(map(o.mul, ls, r+[1])) > 0, lines))
       print ",".join(map(str, r)+['1' if val else '0'])
    

if __name__ == "__main__":
    rand_forest_class([[0,-1,9], [0,1,-1], [1, 0, -1], [-1, 0, 9]], 1000)
