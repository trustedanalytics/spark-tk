"""Test connected_components graphx, Valuesare checked against networkx"""
import unittest

from sparktkregtests.lib import sparktk_test


class ConnectedComponents(sparktk_test.SparkTKTestCase):

    def test_connected_component(self):
        """ Tests the graphx connected components in ATK"""
        super(ConnectedComponents, self).setUp()
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

        components = self.graph.connected_components()
        components.add_columns(
            lambda x: x['Vertex'].split('_')[1], ("element", str))
        frame = components.download(components.count())
        group = frame.groupby('Component').agg(lambda x: x.nunique())

        # Each component should only have 1 element value, the name of the
        # component
        for _, row in group.iterrows():
            self.assertEqual(row['element'], 1)


if __name__ == '__main__':
    unittest.main()
