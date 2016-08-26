""" Tests the weighted_degree on a graph"""

import unittest

from sparktkregtests.lib import sparktk_test

class WeightedDegreeTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Build frames and graphs to be tested."""
        super(WeightedDegreeTest, self).setUp()
        schema_vertex = [("id", str),
                         ("in", int),
                         ("out", int),
                         ("undirectedcount", int),
                         ("isundirected", int),
                         ("outlabel", int),
                         ("insum", float),
                         ("outsum", float),
                         ("undirectedsum", float),
                         ("labelsum", float),
                         ("nolabelsum", float),
                         ("defaultsum", float),
                         ("integersum", int)]

        schema_directed = [("src", str),
                           ("dst", str),
                           ("value", float),
                           ("badvalue", str),
                           ("intvalue", int),
                           ("int64value", int)]

        self.vertex_frame = self.context.frame.import_csv(
            self.get_file("annotate_node_list.csv"), schema=schema_vertex)

        directed_frame = self.context.frame.import_csv(
            self.get_file("annotate_directed_list.csv"),
            schema=schema_directed)

        self.graph = self.context.graph.create(
            self.vertex_frame, directed_frame)

    def test_degree_out(self):
        """Test degree count annotation."""
        graph_result = self.graph.degrees(degree_option="out")
        self._verify_frame(graph_result, "out")

    def test_degree_in(self):
        """Test degree count annotation for in edges."""
        graph_result = self.graph.degrees(degree_option="in")
        self._verify_frame(graph_result, "in")

    def test_degree_undirected(self):
        """Test degree count annotation for undirected edges."""
        graph_result = self.graph.degrees(degree_option="undirected")
        self._verify_frame(graph_result, "undirectedcount")

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
        result = frame.download(frame.count())
        self.vertex_frame.rename_columns({'id': 'Vertex'})
        result.join(self.vertex_frame)

        for _, i in result.iterrows():
            self.assertEqual(i["Degree"], i[second])

if __name__ == "__main__":
    unittest.main()
