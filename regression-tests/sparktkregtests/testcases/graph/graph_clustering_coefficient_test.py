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

        results = result.to_pandas(result.count())

        # The local coefficient was calculated by hand and added as an
        # attribute to the nodes.
        for _, node in results.iterrows():
            self.assertAlmostEqual(node['unlabeled_coeff'], node['clustering_coefficient'], delta=.01)


if __name__ == "__main__":
    unittest.main()
