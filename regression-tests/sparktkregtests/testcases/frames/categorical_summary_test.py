""" Test summary statistics for both numerical and categorical columns.  """
import unittest
from sparktkregtests.lib import sparktk_test

# related bugs:
# @DPNG-9636 - percentage is inaccurate for Nones in categorical summary


class CategoricalSummaryTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Build test frames"""
        super(CategoricalSummaryTest, self).setUp()

        datafile = self.get_file("movie_small.csv")

        schema = [('movie', int),
                  ('vertexType', str),
                  ('user', int),
                  ('rating', int),
                  ('splits', str),
                  ('percent', int),
                  ('predict', int),
                  ('weight', int),
                  ('age', int)]

        # Create basic movie frame
        self.frame = self.context.frame.import_csv(datafile, schema=schema, header=True)

    def test_cat_summary_single_column_defaults(self):
        """Test categorical summary with defaults"""
        stats = self.frame.categorical_summary('age')
        self._top_k("age", stats, 10)

    def test_cat_summary_single_column_top_k(self):
        """Categorical summary with top k"""
        stats = self.frame.categorical_summary('age', top_k= 11)
        self._top_k("age", stats, 11)

    def test_cat_summary_single_column_threshold(self):
        """Categorical summary with threshold """
        stats = self.frame.categorical_summary('age', threshold=.02)
        self._top_threshold("age", stats, .02)

    def test_cat_summary_single_column_top_k_and_threshold(self):
        """Categorical summary with top_k & threshold"""
        stats = self.frame.categorical_summary(
            'age', threshold=.02, top_k=11)
        self._top_threshold("age", stats, 0.02, 11)

    def test_cat_summary_single_column_with_None(self):
        """Categorical summary with Nones"""
        def add_nones(row):
            if int(row['age']) % 5 == 0:      # ending in 0 or 5
                return ""
            if int(row['age']) % 10 == 9:     # ending in 9
                return ""
            if int(row['age']) % 13 == 0:    # multiple of 13
                return ""
            return row['age']

        # Add empty strings to exercise missing functionality
        self.frame.add_columns(add_nones, ('Nones', int))

        stats = self.frame.categorical_summary('Nones', top_k=8)
        self._top_k("Nones", stats, 10)

    def test_cat_summary_multi_column(self):
        """Categorical summary using multiple columns"""
        stats = self.frame.categorical_summary(['movie', 'user', 'age', 'rating', 'weight'],
                top_k=[None, 10, None, 10, 10], threshold=[None, None, 0.02, 0.02, None])
        self.assertEqual(len(stats), 5)
        for i in stats:
            if i.column_name  == 'age':
                self._top_threshold(
                    i.column_name, {"categorical_summary": [i]}, 0.02)
            elif i.column_name == 'rating':
                self._top_threshold(
                    i.column_name, {"categorical_summary": [i]}, 0.02, 10)
            else:
                self._top_k(i.column_name, {"categorical_summary": [i]}, 10)

    def test_cat_summary_invalid_column_name_error(self):
        """Bad column name errors"""
        with self.assertRaisesRegexp(Exception, "Invalid column name"):
            self.frame.categorical_summary(('invalid', {"threshold": .02}))

    def test_cat_summary_invalid_threshold_error_lo(self):
        """Invalid lower threshold errors"""
        with self.assertRaisesRegexp(Exception, "Invalid column name"):
            self.frame.categorical_summary(('age', {"threshold": -1}))

    def test_cat_summary_invalid_threshold_error_hi(self):
        """ Invalid upper threshold errors"""
        with self.assertRaisesRegexp(Exception, "Invalid column name"):
            self.frame.categorical_summary(('age', {"threshold": 1.1}))

    def test_cat_summary_invalid_top_k_error(self):
        """Invalid top_k errors"""
        with self.assertRaisesRegexp(Exception, "Invalid column name"):
            self.frame.categorical_summary(('age', {"top_k": -1}))

    def _top_k(self, column, stats, k):
        # validates top k results and k value
        result = self._compare_equal(column, stats)
        self.assertLessEqual(result[0], k)

    def _top_threshold(self, column, stats, threshold, k=None):
        # validates threshold and top k if given
        result = self._compare_equal(column, stats)
        # check both all values were above threshold percent, and all
        # values above threshold percent are found
        for i in result[3]:
            self.assertGreater(i/result[1], threshold)
        size = filter(lambda x: x/result[1] > threshold, result[2].values)

        if k is None:
            self.assertEqual(len(size), result[0])
        else:
            self.assertLessEqual(result[0], k)

    def _compare_equal(self, column, stats):
        # Group and count the values, drop any ignored values, validate
        pf = self.frame.download(self.frame.row_count)
        # here we do our own analysis to compare with the results of the categorical summary
        expected = pf.groupby(column).size().sort_values(ascending=False)

        sum = float(expected.sum())
        nones = expected.get("", 0)
        expected = expected.drop("", errors="ignore")
        # the way in which the stats result is returned is inconsistent
        # sometimes we have to get it by the key, othertimes there is no categorical_summary key
        if "categorical_summary" in stats:
            catsum = stats["categorical_summary"]
        else:
            catsum = stats
        # I believe the -2 the author wrote here is to not include the bottom "missing" and "other" columns
        k = len(catsum[0].levels)-2
        level_values = []
        self.assertEqual(catsum[0].column_name, column)
        # for each level, compare the result from the categorical_summary frequency and percentage with our own expected values
        for i in catsum[0].levels:
            if str(i.level) == "<Missing>":
                self.assertEqual(i.frequency, nones)
                self.assertAlmostEqual(i.percentage, nones/sum)
            elif str(i.level) == "<Other>":
                self.assertEqual(i.frequency, expected[k:].sum())
                self.assertEqual(i.percentage, (expected[k:].sum()/sum))
            else:
                self.assertEqual(i.frequency, expected[int(i.level)])
                self.assertEqual(i.percentage, expected[int(i.level)]/sum)
                level_values.append(i.frequency)
        return (k, sum, expected, level_values)


if __name__ == "__main__":
    unittest.main()
