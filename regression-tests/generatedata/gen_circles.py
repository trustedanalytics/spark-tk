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

#  Copyright (c) 2016 Intel Corporation 
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

import math
def generate_circles(r, n):
    return [( r * math.cos(2.0 * math.pi * i/float(n)), r * math.sin(2.0 * math.pi * i/float(n)))
             for i in xrange(n)]

def sim(x, y):
    dist = (x[0] - y[0])**2 + (x[1] - y[1])**2
    return math.exp(-dist/2.0)

def get_sims(points, n):
    for i in xrange(1, n):
        for j in xrange(i):
            yield [i, j, sim(points[i], points[j])]

points = generate_circles(1, 10) + generate_circles(4, 10)
n = 20

with open("pic_data.csv", "w") as f:
    for s in get_sims(points, n):
        f.write(str(s[0]) + "," + str(s[1]) + "," + str(s[2]) + "\n")
    
