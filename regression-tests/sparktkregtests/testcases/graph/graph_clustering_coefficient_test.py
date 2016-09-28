"""Tests the clustering coefficient functionality"""
import unittest

from sparktkregtests.lib import sparktk_test


class GraphClusteringCoefficient(sparktk_test.SparkTKTestCase):

    def test_clustering_coefficient(self):
        """ test the output of clustering coefficient"""
        schema_vertex = [("id", str),
                         ("labeled_coeff", float),
                         ("unlabeled_coeff", float)]

        node_frame = self.context.frame.import_csv(
            self.get_file("clustering_graph_vertices.csv"),
            schema=schema_vertex)

        schema_undirected_label = [("src", str),
                                   ("dst", str),
                                   ("labeled", str)]

        main_edge_frame = self.context.frame.import_csv(
            self.get_file("clustering_graph_edges.csv"), schema=schema_undirected_label)

        graph = self.context.graph.create(node_frame, main_edge_frame)

        result = graph.clustering_coefficient()

        results = result.download(result.count())

        # The local coefficient was calculated by hand and added as an
        # attribute to the nodes.
        for _, node in results.iterrows():
            self.assertAlmostEqual(node['unlabeled_coeff'], node['Clustering_Coefficient'], delta=.01)


if __name__ == "__main__":
    unittest.main()
