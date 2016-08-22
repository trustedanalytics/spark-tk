""" test cases for the graph system

    THIS TEST REQUIRES NO THIRD PARTY APPLICATIONS OTHER THAN THE ATK
    THIS TEST IS TO BE MAINTAINED AS A SMOKE TEST FOR THE ML SYSTEM
"""
import unittest
import os

import sparktk as stk


class GraphDocTest(unittest.TestCase):

    def test_graph_example(self):
        """Documentation test for classifiers"""
        # Get a context from the spark-tk library
        ctxt = stk.TkContext()
        # Graphs are composed of 2 sets, one of nodes, and one of edges that
        # connect exactly two (possibly not distinct) nodes. The degree
        # of a node is the number of edges attached to it

        # Below we build a frame using a nodelist and an edgelist.

        node_frame = ctxt.frame.create(
            [["node1"],
             ["node2"],
             ["node3"],
             ["node4"],
             ["node5"]],
             [("id", str)])
        edge_frame = ctxt.frame.create(
            [["node2", "node3"],
             ["node2", "node1"],
             ["node2", "node4"],
             ["node2", "node5"]],
             [("src", str), ("dst", str)])

        # The graph is a center node on node2, with 4 nodes each attached to
        # the center node. This is known as a star graph, in this configuration
        # it can be visualized as a plus sign

        # To Create a graph first you define the nodes, and then the edges

        graph = ctxt.graph.create(node_frame, edge_frame)

        # get the degrees, which have known values
        degrees = graph.degrees()

        degree_list = degrees.take(5).data
        known_list = [[u'node4', 1],
                      [u'node1', 1],
                      [u'node5', 1],
                      [u'node2', 4],
                      [u'node3', 1]]

        self.assertItemsEqual(known_list, degree_list)


if __name__ == '__main__':
    unittest.main()
