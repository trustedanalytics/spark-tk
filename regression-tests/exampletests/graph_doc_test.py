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

""" test cases for the graph system

    THIS TEST REQUIRES NO THIRD PARTY APPLICATIONS OTHER THAN THE SPARKTK
    THIS TEST IS TO BE MAINTAINED AS A SMOKE TEST FOR THE ML SYSTEM
"""
import unittest

import sparktk


class GraphDocTest(unittest.TestCase):

    def test_graph_example(self):
        """Documentation test for classifiers"""
        # Get a context from the spark-tk library
        tc = sparktk.TkContext()
        # Graphs are composed of 2 sets, one of verticess, and one of edges
        # that connect exactly two (possibly not distinct) verticees.
        # The degree of a vertex is the number of edges attached to it

        # Below we build a frame using a vertex list and an edge list.

        vertex_frame = tc.frame.create(
            [["vertex1"],
             ["vertex2"],
             ["vertex3"],
             ["vertex4"],
             ["vertex5"]],
            [("id", str)])
        edge_frame = tc.frame.create(
            [["vertex2", "vertex3"],
             ["vertex2", "vertex1"],
             ["vertex2", "vertex4"],
             ["vertex2", "vertex5"]],
            [("src", str), ("dst", str)])

        # The graph is a center vertex on vertex2, with 4 verticess each
        # attached to the center vertex . This is known as a star graph, in
        # this configuration it can be visualized as a plus sign

        # To Create a graph first you define the vertices, and then the edges

        graph = tc.graph.create(vertex_frame, edge_frame)

        # get the degrees, which have known values
        degrees = graph.degrees()

        degree_list = degrees.take(5)
        known_list = [[u'vertex4', 1],
                      [u'vertex1', 1],
                      [u'vertex5', 1],
                      [u'vertex2', 4],
                      [u'vertex3', 1]]

        self.assertItemsEqual(known_list, degree_list)


if __name__ == '__main__':
    unittest.main()
