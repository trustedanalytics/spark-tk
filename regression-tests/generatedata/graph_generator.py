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

"""generates a csv file with randomized graph data"""
import random
import copy


def generate_graph(num_vert, num_edges):
    vertices = [x for x in range(0, num_vert)]
    vertex_lines = ""
    for x in vertices:
        vertex_lines = vertex_lines + str(x) + "\n"
    with open("../datasets/random_graph_vertices.csv", "w") as vertex_file:
        vertex_file.write(vertex_lines)

    edges = []
    edge_lines = ""
    remaining_vertices = vertices
    for x in range(0, num_edges):
        rand_src_vert = int(random.choice(vertices))
        remaining_vertices = copy.copy(vertices)
        remaining_vertices.remove(rand_src_vert)
        rand_dest_vert = int(random.choice(remaining_vertices))
        edge_line = str(rand_src_vert) + "," + str(rand_dest_vert) + "\n"
        edge_lines = edge_lines + edge_line

    with open("../datasets/random_graph_edges.csv", "w") as edges_file:
        edges_file.write(edge_lines)
