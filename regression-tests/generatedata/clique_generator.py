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


def k_gen(src, n, prefix):
    return [(prefix+str(src), prefix+str(dst+1)) for dst in range(n)]

def generate_cliques(clique_count):

    for i in range(2, clique_count):
        edges = [k_gen(j, j-1, "k_"+str(i)+"_") for j in range(2, i+1)]
        flattened_edges = [inner for outer in edges for inner  in outer]
        for (i, j) in flattened_edges:
            print str(i)+","+str(j)


        

if __name__ == "__main__":
    generate_cliques(11)
