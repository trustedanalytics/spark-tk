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

        triangles = result.download(result.count())
        # Create a dictionary of triangle count per node:
        dictionary_of_triangle_count = {i['Vertex']: (i['Triangles'])
                                        for (_, i) in triangles.iterrows()}

        edge_list = self.frame.take(
            n=self.frame.count(), columns=['src', 'dst']).data

        # build the network x result
        g = nx.Graph()
        g.add_edges_from(edge_list)
        triangle_counts_from_networkx = nx.triangles(g)

        self.assertEqual(
            dictionary_of_triangle_count, triangle_counts_from_networkx)

if __name__ == '__main__':
    unittest.main()
