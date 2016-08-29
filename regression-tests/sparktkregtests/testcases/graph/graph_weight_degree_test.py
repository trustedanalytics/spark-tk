""" Tests the weighted_degree on a graph"""

import unittest

from sparktkregtests.lib import sparktk_test

class WeightedDegreeTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Build frames and graphs to be tested"""
        super(WeightedDegreeTest, self).setUp()
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

        self.frame.add_columns(lambda x: 2, ("value", int))

        self.graph = self.context.graph.create(self.vertices, self.frame)

    def test_annotate_weight_degree_out(self):
        """Test degree count weighted on out edges"""
        degree_weighted = self.graph.weighted_degrees("value", "out")
        res = degree_weighted.download(degree_weighted.count())
        for _, row in res.iterrows():
            row_val = row['Vertex'].split('_')
            self.assertEqual(2*(int(row_val[2])-1), row['Degree'])

    def test_weight_degree_in(self):
        """Test degree count weighted on in edges"""
        degree_weighted = self.graph.weighted_degrees("value", "in")
        res = degree_weighted.download(degree_weighted.count())
        for _, row in res.iterrows():
            row_val = row['Vertex'].split('_')
            self.assertEqual(
                2*(int(row_val[1])-int(row_val[2])), row['Degree'])

    def test_weight_degree_undirected(self):
        """Test degree count weighted on undirected edges"""
        degree_weighted = self.graph.weighted_degrees("value", "undirected")
        res = degree_weighted.download(degree_weighted.count())
        for _, row in res.iterrows():
            row_val = row['Vertex'].split('_')
            self.assertEqual(2*(int(row_val[1])-1), row['Degree'])

    def test_weight_type_error(self):
        """Test degree count weighted with type error."""
        with self.assertRaisesRegexp(TypeError, "unexpected keyword"):
            self.graph.weighted_degrees(edge_weight_property="badvalue")

    def test_weight_non_value(self):
        """Test degree count weighted with type error"""
        with self.assertRaisesRegexp(TypeError, "unexpected keyword"):
            self.graph.weighted_degrees(edge_weight_property="nonvalue")


if __name__ == "__main__":
    unittest.main()
