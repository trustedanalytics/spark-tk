""" Tests the ECDF functionality """
import unittest
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))
from qalib import sparktk_test


class ecdfTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Build test frame"""
        super(ecdfTest, self).setUp()
        dataset_33 = self.get_file("model_33_percent.csv")
        schema_33 = [("user_id", int),
                     ("vertex_type", str),
                     ("movie_id", int),
                     ("rating", int),
                     ("splits", str),
                     ("predicted", int)]

        self.frame_33_percent = self.context.frame.import_csv(dataset_33, schema=schema_33)

    def test_33_percent(self):
        """Perform the ecdf test on the 33 percent data."""
        frame_ecdf = self.frame_33_percent.ecdf('predicted')
        self._validate_ecdf(frame_ecdf, self.frame_33_percent, 'predicted')

    def test_1_2_5(self):
        """ Perform the ecdf test on the 1 2 5 data  """
        dataset_netf = self.get_file("netf_1_2_5.csv")
        schema_netf = [("user_id", int),
                       ("vertex_type", str),
                       ("movie_id", int),
                       ("rating", int),
                       ("splits", str)]
        frame_netf = self.context.frame.import_csv(dataset_netf, schema=schema_netf)
        frame_ecdf = frame_netf.ecdf("rating")
        self._validate_ecdf(frame_ecdf, frame_netf, 'rating')

    def _validate_ecdf(self, frame_ecdf, frame, column):
        pd_ecdf = frame_ecdf.download(frame_ecdf.row_count)
        pd_frame = frame.download(frame.row_count)

        grouped = pd_frame.groupby(column).size()
        result = grouped.sort_index().cumsum()*1.0/len(pd_frame)
        for _, i in pd_ecdf.iterrows():
            self.assertAlmostEqual(i[column+'_ecdf'], result[int(i[column])])

    def test_ecdf_bad_name(self):
        """Test ecdf with an invalid column name."""
        with self.assertRaises(Exception):
            self.frame_33_percent.ecdf("bad_name")

    def test_ecdf_bad_type(self):
        """Test ecdf with an invalid column type."""
        with self.assertRaises(Exception):
            self.frame_33_percent.ecdf(5)

    def test_ecdf_none(self):
        """Test ecdf with a None for the column name."""
        with self.assertRaises(Exception):
            self.frame_33_percent.ecdf(None)

if __name__ == '__main__':
    unittest.main()
