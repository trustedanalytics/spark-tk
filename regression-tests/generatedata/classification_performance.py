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

# vim: set encoding=utf-8 
"""binary classification metrics dataset generator"""

def generate_data(rows, positive, correct):
    tp = rows * positive * correct
    fp = rows * (1-positive) * (1-correct)
    tn = rows * (1-positive) * correct
    fn = rows * positive * (1-correct)

    for i in range(int(tp)):
        print "1,1"
    for i in range(int(fp)):
        print "0,1"
    for i in range(int(tn)):
        print  "0,0"
    for i in range(int(fn)):
        print "1,0"

if __name__ == "__main__":
    generate_data(2*10**2, 0.4, .8)
