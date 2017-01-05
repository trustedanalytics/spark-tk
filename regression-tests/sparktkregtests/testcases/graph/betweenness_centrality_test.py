"""Tests betweenness centrality algorithm for graphs"""
import unittest
import networkx as nx
from sparktkregtests.lib import sparktk_test

class BetweennessCentrality(sparktk_test.SparkTKTestCase):

    def setUp(self):
        edges = self.context.frame.create(
            [(0, 1, 1),
            (0, 2, 1),
            (2, 3, 2),
            (2, 4, 4),
            (3, 4, 2),
            (3, 5, 4),
            (4, 5, 2),
            (4, 6, 1)],
            ["src", "dst", "weights"])

        vertices = self.context.frame.create(
            [[0], [1], [2], [3], [4], [5], [6]], ["id"])

        self.graph = self.context.graph.create(vertices, edges)

    @unittest.skip("")
    def test_default(self):
        """Test default settings"""
        result_frame = self.graph.betweenness_centrality()
        result = result_frame.to_pandas()

        #validate centrality values
        expected_value = [0.1, 0.333, 0.433, 0.0, 0.0, 0.0, 0.533]

        for i, row in result.iterrows():
            self.assertAlmostEqual(
                row["betweenness_centrality"],
                expected_value[i],
                delta = 0.001)

    def weighted_G(self):
        G=nx.Graph()
        G.add_edge(0,1,weight=1)
        G.add_edge(0,2,weight=1)
        G.add_edge(2,3,weight=2)
        G.add_edge(2,4,weight=4)
        G.add_edge(3,4,weight=2)
        G.add_edge(3,5,weight=4)
        G.add_edge(4,5,weight=2)
        G.add_edge(4,6,weight=1)
        return G

    def test_weights_single_shortest_path(self):
        """Tests weighted betweenness when only one shortest path present""" 
        edges = self.context.frame.create(
            [(0,1,3), (0, 2, 2),
            (0, 3, 6), (0, 4, 4),
            (1, 3, 5), (1, 5, 5),
            (2, 4, 1), (3, 4, 2),
            (3, 5, 1), (4, 5, 4)],
            ["src", "dst", "weights"])
        vertices = self.context.frame.create([[0], [1], [2], [3], [4], [5]], ["id"])
        graph = self.context.graph.create(vertices, edges)

        #validate against values from networkx betweenness centrality
        expected_values = [0.0, 2.0, 0.0, 4.0, 3.0, 4.0]
        result_frame = graph.betweenness_centrality("weights", False)
        result = result_frame.to_pandas()

        for i, row in result.iterrows():
            self.assertAlmostEqual(
                row["betweenness_centrality"],
                expected_values[i],
                delta = 0.1)

    @unittest.skip("")
    def test_weights(self):
        """Test betweenness with weighted cost"""
        result_frame = self.graph.betweenness_centrality("weights", False)
        G = self.weighted_G()
        b = nx.betweenness_centrality(G,
                weight='weight',
                normalized=False)
        b_n = nx.betweenness_centrality(G,
                weight='weight',
                normalized=True)
        #print "Networkx:\nUnnormalized:{0}\nNormalized:{1}\nSparktk:\n{2}".format(b, b_n, result_frame.inspect(10))

    def test_normalize(self):
        """Test unnomallized betweenness crentrality"""
        result_frame = self.graph.betweenness_centrality(normalize=False)
        result = result_frame.to_pandas()

        #validate centrality values
        expected_values = [0.0, 5.0, 0.0, 0.0, 8.0, 1.5, 6.5]
        for i, row in result.iterrows():
            self.assertAlmostEqual(
                row["betweenness_centrality"],
                expected_values[i],
                delta = 0.1)

    def test_bad_weights_column_name(self):
        """Should throw exception when bad weights column name given"""
        with self.assertRaisesRegexp(
                Exception, "Field \"BAD\" does not exist"):
            self.graph.betweenness_centrality("BAD")
if __name__ == "__main__":
    unittest.main()
