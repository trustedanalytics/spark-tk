""" Tests label propagation, community detetction by association"""
import unittest

from sparktkregtests.lib import sparktk_test


class LbpGraphx(sparktk_test.SparkTKTestCase):

    def test_label_propagation(self):
    """label propagation on plus sign, deterministic, not conververgent"""
        vertex_frame = self.context.frame.create(
                          [["vertex1"],
                           ["vertex2"],
                           ["vertex3"],
                           ["vertex4"],
                           ["vertex5"]],
                          [("id", str)])
        edge_frame = self.context.frame.create(
                          [["vertex2", "vertex3"],
                           ["vertex2", "vertex1"],
                           ["vertex2", "vertex4"],
                           ["vertex2", "vertex5"]],
                          [("src", str), ("dst", str)])

        graph = self.context.graph.create(vertex_frame, edge_frame)

        community = graph.label_propagation(10)

        communities = community.take(5).data
        self.assertItemsEqual([["vertex1", 0],
                               ["vertex2", 1],
                               ["vertex3", 0],
                               ["vertex4", 0],
                               ["vertex5", 0]], communities)

        community = graph.label_propagation(11)

        communities = community.take(5).data
        self.assertItemsEqual([["vertex1", 1],
                               ["vertex2", 0],
                               ["vertex3", 1],
                               ["vertex4", 1],
                               ["vertex5", 1]], communities)


if __name__ == '__main__':
    unittest.main()
