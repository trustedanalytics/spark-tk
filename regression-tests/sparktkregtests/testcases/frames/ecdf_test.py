""" Tests the ECDF functionality """
import unittest
import random
from sparktkregtests.lib import sparktk_test


class ecdfTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Build test frame"""
        super(ecdfTest, self).setUp()

        # generate a dataset to test ecdf on
        # it will just be a single column of ints
        column = []
        for index in range(0, 20):
            column.append([random.randint(0, 5)])
        schema = [("C0", int)]

        self.frame = self.context.frame.create(column,
                                               schema=schema)

    def validate_ecdf(self):
        # call sparktk ecdf function on the data and download result
        ecdf_sparktk_result = self.frame.ecdf("C0")
        pd_ecdf = ecdf_sparktk_result.download(ecdf_sparktk_result.row_count)
        # download the original frame so we can calculate our own result
        pd_original_frame = self.frame.download(self.frame.row_count)

        # the formula for calculating ecdf is
        # F(x) = 1/n * sum from 1 to n of I(x_i)
        # where I = { 1 if x_i <= x, 0 if x_i > x }
        # i.e., for each element in our data column count
        # the number of items in that row which are less than
        # or equal to that item, divide by the number
        # of total items in the column
        grouped = pd_original_frame.groupby("C0").size()
        our_result = grouped.sort_index().cumsum()*1.0/len(pd_original_frame)

        # finaly we iterate through the sparktk result and compare it with our result
        for index, row in pd_ecdf.iterrows():
            self.assertAlmostEqual(row["C0"+'_ecdf'],
                                   our_result[int(row["C0"])])

    def test_ecdf_bad_name(self):
        """Test ecdf with an invalid column name."""
        with self.assertRaisesRegexp(Exception, "No column named bad_name"):
            self.frame.ecdf("bad_name")

    def test_ecdf_bad_type(self):
        """Test ecdf with an invalid column type."""
        with self.assertRaisesRegexp(Exception, "does not exist"):
            self.frame.ecdf(5)

    def test_ecdf_none(self):
        """Test ecdf with a None for the column name."""
        with self.assertRaisesRegexp(Exception, "column is required"):
            self.frame.ecdf(None)


if __name__ == '__main__':
    unittest.main()
