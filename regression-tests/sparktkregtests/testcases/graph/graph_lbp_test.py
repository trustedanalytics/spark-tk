""" Tests LBP Graphx implementation by comparing results agains graphlab """
import unittest

from sparktkregtests.lib import sparktk_test


class LbpPottsModel(sparktk_test.SparkTKTestCase):

    def test_lbp_cross_3_state(self):
        """Test 3 state Potts model"""
        vertex_frame = self.context.frame.create(
                          [[1, "1.0 0.0 0.0"],
                           [2, ".3 .3 .3"],
                           [3, "1.0 0.0 0.0"],
                           [4, "0.0 1.0 0.0"],
                           [5, "0.0 0.0 1.0"]],
                          [("id", int),
                           ("vertex_weight", str)])
        edge_frame = self.context.frame.create(
                          [[2, 3, 1.0],
                           [2, 1, 1.0],
                           [2, 4, 1.0],
                           [2, 5, 1.0]],
                          [("src", int),
                           ("dst", int),
                           ("weight", float)])

        graph = self.context.graph.create(vertex_frame, edge_frame)

        potts = graph.loopy_belief_propagation("vertex_weight", "weight", 2)

        known_vals = {1: (1.0, 0.0, 0.0),
                      2: (0.57611688, 0.2119415576, 0.2119415576),
                      3: (1.0, 0.0, 0.0),
                      4: (0.0, 1.0, 0.0),
                      5: (0.0, 0.0, 1.0)}
        potts_vals = potts.to_pandas(potts.count())

        for _, row in potts_vals.iterrows():
            values = map(float, row["Posterior"][1:-1].split(","))
            self.assertAlmostEqual(known_vals[row["id"]][0], values[0])
            self.assertAlmostEqual(known_vals[row["id"]][1], values[1])
            self.assertAlmostEqual(known_vals[row["id"]][2], values[2])

    def test_lbp_cross_50(self):
        """Test a balanced graph"""
        vertex_frame = self.context.frame.create(
                          [[1, "1.0 0.0"],
                           [2, ".5 .5"],
                           [3, "1.0 0.0"],
                           [4, "0.0 1.0"],
                           [5, "0.0 1.0"]],
                          [("id", int),
                           ("vertex_weight", str)])
        edge_frame = self.context.frame.create(
                          [[2, 3, 1.0],
                           [2, 1, 1.0],
                           [2, 4, 1.0],
                           [2, 5, 1.0]],
                          [("src", int),
                           ("dst", int),
                           ("weight", float)])

        graph = self.context.graph.create(vertex_frame, edge_frame)

        potts = graph.loopy_belief_propagation("vertex_weight", "weight", 2)

        known_vals = {1: (1.0, 0.0),
                      2: (0.5, 0.5),
                      3: (1.0, 0.0),
                      4: (0.0, 1.0),
                      5: (0.0, 1.0)}
        potts_vals = potts.to_pandas(potts.count())

        for _, row in potts_vals.iterrows():
            values = map(float, row["Posterior"][1:-1].split(","))
            self.assertAlmostEqual(known_vals[row["id"]][0], values[0])
            self.assertAlmostEqual(known_vals[row["id"]][1], values[1])

    def test_lbp_cross_3_1(self):
        """Test LBP on a cross with a 3-1 split on the distribution"""
        vertex_frame = self.context.frame.create(
                          [[1, "1.0 0.0"],
                           [2, "0.5 0.5"],
                           [3, "1.0 0.0"],
                           [4, "0.0 1.0"],
                           [5, "1.0 0.0"]],
                          [("id", int),
                           ("vertex_weight", str)])
        edge_frame = self.context.frame.create(
                          [[2, 3, 1.0],
                           [2, 1, 1.0],
                           [2, 4, 1.0],
                           [2, 5, 1.0]],
                          [("src", int),
                           ("dst", int),
                           ("weight", float)])

        graph = self.context.graph.create(vertex_frame, edge_frame)


        potts = graph.loopy_belief_propagation("vertex_weight", "weight", 2)

        known_vals = {1: (1.0, 0.0),
                      2: (0.88079707798, 0.119202922),
                      3: (1.0, 0.0),
                      4: (0.0, 1.0),
                      5: (1.0, 0.0)}

        potts_vals = potts.to_pandas(potts.count())

        for _, row in potts_vals.iterrows():
            values = map(float, row["Posterior"][1:-1].split(","))
            self.assertAlmostEqual(known_vals[row["id"]][0], values[0])
            self.assertAlmostEqual(known_vals[row["id"]][1], values[1])

    def test_lbp_cross(self):
        """Test lbp on a basic cross with a 4-0 split"""
        vertex_frame = self.context.frame.create(
                          [["1", "1.0 0.0"],
                           ["2", ".5 .5"],
                           ["3", "1.0 0.0"],
                           ["4", "1.0 0.0"],
                           ["5", "1.0 0.0"]],
                          [("id", str), ("vertex_weight", str)])
        edge_frame = self.context.frame.create(
                          [["2", "3", 0.5],
                           ["2", "1", 0.5],
                           ["2", "4", 0.5],
                           ["2", "5", 0.5]],
                          [("src", str),
                           ("dst", str),
                           ("weight", float)])

        graph = self.context.graph.create(vertex_frame, edge_frame)

        potts = graph.loopy_belief_propagation("vertex_weight", "weight", 2)

        known_vals = {"1": (1.0, 0.0),
                      "2": (0.88079707797, 0.11920292202),
                      "3": (1.0, 0.0),
                      "4": (1.0, 0.0),
                      "5": (1.0, 0.0)}

        potts_vals = potts.to_pandas(potts.count())

        for _, row in potts_vals.iterrows():
            values = map(float, row["Posterior"][1:-1].split(","))
            self.assertAlmostEqual(known_vals[row["id"]][0], values[0])
            self.assertAlmostEqual(known_vals[row["id"]][1], values[1])


    def test_lbp_double_cross(self):
        """Test lbp on a double cross"""
        vertex_frame = self.context.frame.create(
                          [["1", "1.0 0.0", 1, "1.0 0.0"],
                           ["2", "0.5 0.5", 0, ""],
                           ["3", "1.0 0.0", 1, "1.0 0.0"],
                           ["4", "0.0 1.0", 1, "0.0 1.0"],
                           ["5", "0.5 0.5", 0, ""],
                           ["6", "0.0 1.0", 1, "0.0 1.0"],
                           ["7", "0.0 1.0", 1, "0.0 1.0"],
                           ["8", "1.0 0.0", 1, "1.0 0.0"]],
                          [("id", str),
                           ("vertex_weight", str),
                           ("is_observed",int), ("label", str)])
        edge_frame = self.context.frame.create(
                          [["2", "3", 1.0],
                           ["2", "1", 1.0],
                           ["2", "4", 1.0],
                           ["2", "5", 1.0],
                           ["6", "5", 1.0],
                           ["7", "5", 1.0],
                           ["8", "5", 1.0]],
                          [("src", str),
                           ("dst", str),
                           ("weight", float)])

        graph = self.context.graph.create(vertex_frame, edge_frame)

        potts = graph.loopy_belief_propagation("vertex_weight", "weight", 2)


        known_vals = {"1": (1.0, 0.0),
                      "2": (0.6378903114, 0.36210968),
                      "3": (1.0, 0.0),
                      "4": (0.0, 1.0),
                      "5": (0.36210968, 0.6378903114),
                      "6": (0.0, 1.0),
                      "7": (0.0, 1.0),
                      "8": (1.0, 0.0)}
        potts_vals = potts.to_pandas(potts.count())

        for _, row in potts_vals.iterrows():
            values = map(float, row["Posterior"][1:-1].split(","))
            self.assertAlmostEqual(known_vals[row["id"]][0], values[0])
            self.assertAlmostEqual(known_vals[row["id"]][1], values[1])



if __name__ == '__main__':
    unittest.main()
