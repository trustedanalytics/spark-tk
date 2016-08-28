""" Tests the weighted_degree on a graph"""

import unittest

from sparktkregtests.lib import sparktk_test

class WeightedDegreeTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Build frames and graphs to be tested."""
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

        self.graph = self.context.graph.create(self.vertices, self.frame)

    def test_degree_out(self):
        """Test degree count annotation."""
        graph_result = self.graph.degrees(degree_option="out")
        res = graph_result.download(graph_result.count())
        for _, row in res.iterrows():
            row_val = row['Vertex'].split('_')
            self.assertEqual(int(row_val[2])-1, row['Degree'])

    def test_degree_in(self):
        """Test degree count annotation for in edges."""
        graph_result = self.graph.degrees(degree_option="in")
        res = graph_result.download(graph_result.count())
        for _, row in res.iterrows():
            row_val = row['Vertex'].split('_')
            self.assertEqual(int(row_val[1])-int(row_val[2]), row['Degree'])

    def test_degree_undirected(self):
        """Test degree count annotation for undirected edges."""
        graph_result = self.graph.degrees(degree_option="undirected")
        res = graph_result.download(graph_result.count())
        for _, row in res.iterrows():
            row_val = row['Vertex'].split('_')
            self.assertEqual(int(row_val[1])-1, row['Degree'])

    def test_annotate_weight_degree_in(self):
        """Test degree count annotation weighted on in edges."""
        annotated_dict = self.graph.weighted_degrees("value", "in")
        self._verify_frame(annotated_dict, "insum")

    def test_annotate_weight_degree_out(self):
        """Test degree count annotation weighted on out edges."""
        annotated_dict = self.graph.weighted_degrees("value", "out")
        self._verify_frame(annotated_dict, "outsum")

    def test_annotate_weight_degree_undirected(self):
        """Test degree count annotation weighted on undirected edges."""
        annotated_dict = self.graph.weighted_degrees("value", "undirected")
        self._verify_frame(annotated_dict, "undirectedsum")

    def test_annotate_weight_default(self):
        """Test degree count annotation weighted with default value."""
        annotated_dict = self.graph.weighted_degrees(
            edge_weight="value", default_weight=0.5)
        self._verify_frame(annotated_dict, "defaultsum")

    def test_annotate_weight_integer(self):
        """Test degree count annotation weighted with default value."""
        annotated_dict = self.graph.weighted_degrees(
            edge_weight="intvalue", degree_option="in", default_weight=0)
        self._verify_frame(annotated_dict, "integersum")

    def test_annotate_weight_integer64(self):
        """Test degree count annotation weighted with default value."""
        annotated_dict = self.graph.weighted_degrees(
            "int64value", degree_option="in")
        self._verify_frame(annotated_dict, "integersum")

    def test_annotate_weight_type_error(self):
        """Test degree count annotation weighted with type error."""
        with self.assertRaises(Exception):
            self.graph.weighted_degrees(edge_weight_property="badvalue")

    def test_annotate_weight_non_value(self):
        """Test degree count annotation weighted with type error."""
        with self.assertRaises(Exception):
            self.graph.weighted_degrees(edge_weight_property="nonvalue")

    def test_degree_no_name(self):
        """Fails when given no property name."""
        with self.assertRaises(Exception):
            self.graph.degrees("")

    def _verify_frame(self, frame, second):
        # isundirected is either a 0 or a 1, booleans are not supported
        self.vertex_frame.rename_columns({'id': 'Vertex'})
        val = frame.join_inner(self.vertex_frame, 'Vertex')
        print frame.inspect()
        print val.inspect()
        result = val.download(val.count())

        for _, i in result.iterrows():
            self.assertEqual(i["Degree"], i[second])

if __name__ == "__main__":
    unittest.main()
