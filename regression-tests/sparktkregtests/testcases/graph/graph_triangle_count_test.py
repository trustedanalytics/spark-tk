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

"""Tests triangle count for ATK against the networkx implementation"""

import unittest

import networkx as nx

from sparktkregtests.lib import sparktk_test


class TriangleCount(sparktk_test.SparkTKTestCase):

    def test_triangle_counts(self):
        """Build frames and graphs to exercise"""
        super(TriangleCount, self).setUp()
        graph_data = self.get_file("clique_10.csv")
        schema = [('src', str),
                  ('dst', str)]

        # set up the vertex frame, which is the union of the src and
        # the dst columns of the edges
        self.frame = self.context.frame.import_csv(graph_data, schema=schema)
        self.vertices = self.frame.copy()
        self.vertices2 = self.frame.copy()
        self.vertices.rename_columns({"src": "id"})
        self.vertices.drop_columns(["dst"])
        self.vertices2.rename_columns({"dst": "id"})
        self.vertices2.drop_columns(["src"])
        self.vertices.append(self.vertices2)
        self.vertices.drop_duplicates()

        self.graph = self.context.graph.create(self.vertices, self.frame)

        result = self.graph.triangle_count()

        triangles = result.to_pandas(result.count())
        # Create a dictionary of triangle count per triangle:
        dictionary_of_triangle_count = {vertex['id']: (vertex['count'])
                                        for (index, vertex) in triangles.iterrows()}

        edge_list = self.frame.take(
            n=self.frame.count(), columns=['src', 'dst'])

        # build the network x result
        g = nx.Graph()
        g.add_edges_from(edge_list)
        triangle_counts_from_networkx = nx.triangles(g)

        self.assertEqual(
            dictionary_of_triangle_count, triangle_counts_from_networkx)

if __name__ == '__main__':
    unittest.main()
