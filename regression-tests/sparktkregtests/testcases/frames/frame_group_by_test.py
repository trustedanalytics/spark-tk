""" Test functionality of group_by, including aggregation_arguments """
import unittest
import pandas as pd
import numpy as np
import math

from sparktkregtests.lib import sparktk_test


class GroupByTest(sparktk_test.SparkTKTestCase):

    # Aggregates and names for non-numeric aggregates
    # (some aggregates are not defined on integers)
    # atk aggregates, then numpy aggregates

    pd_cols_str = ['size', '<lambda>', 'max', 'min']

    numpy_aggs_str = ['size',
                      lambda x: pd.Series.nunique(x, False),
                      'max',
                      'min']

    atk_cols_str = ['_COUNT', '_COUNT_DISTINCT', '_MAX', '_MIN']


    pd_cols = ['mean', 'size', '<lambda>', 'max',
               'min', 'std', 'nansum', 'var']

    numpy_aggs = ['mean',
                  'size',
                  lambda x: pd.Series.nunique(x, False),
                  'max',
                  'min',
                  'std',
                  np.nansum,
                  'var']

    atk_cols = ['_AVG', '_COUNT', '_COUNT_DISTINCT', '_MAX',
                '_MIN', '_STDEV', '_SUM', '_VAR']

    def setUp(self):
        """Build test frame"""
        super(GroupByTest, self).setUp()
        # Aggregates to test on strings
        self.aggs_str = [self.context.agg.count,
                         self.context.agg.count_distinct,
                         self.context.agg.max,
                         self.context.agg.min]

        # Aggregates for numeric columns
        self.aggs = [self.context.agg.avg,
                     self.context.agg.count,
                     self.context.agg.count_distinct,
                     self.context.agg.max,
                     self.context.agg.min,
                     self.context.agg.stdev,
                     self.context.agg.sum,
                     self.context.agg.var]

        schema_colors = [("Int32_0_15", int),
                         ("Int32_0_31", int),
                         ("colors", str),
                         ("Int64_0_15", int),
                         ("Int64_0_31", int),
                         ("Float32_0_15", float),
                         ("Float32_0_31", float),
                         ("Float64_0_15", float),
                         ("Float64_0_31", float)]

        dataset = self.get_file("colors_32_9cols_128rows.csv")

        self.frame = self.context.frame.import_csv(
            dataset, schema=schema_colors)

    def test_stats_on_string_avg(self):
        """Non-numeric aggregates error on non-numeric column"""
        with self.assertRaises(Exception):
            self.frame.group_by('colors', {'colors': self.context.agg.avg})

    def test_stats_on_string_stdev(self):
        """Non-numeric aggregates error on non-numeric column"""
        with self.assertRaises(Exception):
            self.frame.group_by('colors', {'colors': self.context.agg.stdev})

    def test_stats_on_string_sum(self):
        """Non-numeric aggregates error on non-numeric column"""
        with self.assertRaises(Exception):
            self.frame.group_by('colors', {'colors': self.context.agg.sum})

    def test_stats_on_string_var(self):
        """Non-numeric aggregates error on non-numeric column"""
        with self.assertRaises(Exception):
            self.frame.group_by('colors', {'colors': self.context.agg.var})

    def test_invalid_column_name(self):
        """Aggregate on non-existant column errors"""
        with self.assertRaises(Exception):
            self.frame.group_by(
                'InvalidColumnName', {'colors': self.context.agg.var})

    def test_group_int32_standard(self):
        """Test groupby on 1 column, int32"""
        stats = self.frame.group_by(['Int32_0_15'], {'Int32_0_31': self.aggs})
        self._validate(stats, 'Int32_0_31', ['Int32_0_15'])

    def test_group_float32_standard(self):
        """Test groupby on 1 column, float32"""
        stats = self.frame.group_by(
            ['Float32_0_15'], {'Float32_0_31': self.aggs})
        self._validate(stats, 'Float32_0_31', ['Float32_0_15'])

    def test_group_float64_standard(self):
        """Test groupby on 1 column, float64"""
        stats = self.frame.group_by(
            ['Float64_0_15'], {'Float64_0_31': self.aggs})
        self._validate(stats, 'Float64_0_31', ['Float64_0_15'])

    def test_group_int64_standard(self):
        """Test groupby on 1 column, int64"""
        stats = self.frame.group_by(['Int64_0_15'], {'Int64_0_31': self.aggs})
        self._validate(stats, 'Int64_0_31', ['Int64_0_15'])

    def Test_group_by_str_standard(self):
        """Test groupby on 1 column, string"""
        stats = self.frame.group_by(['colors'], {'Int32_0_31': self.aggs})
        self._validate_str(stats, 'Int32_0_31', ['colors'])

    def test_group_by_str_agg_str(self):
        """Test groupby on 1 column, string, aggregate is string"""
        stats = self.frame.group_by(['colors'], {'colors': self.aggs_str})
        self._validate_str(stats, 'colors', ['colors'])

    def test_group_int32_multiple_cols(self):
        """Test groupby on multiple columns, int32"""
        stats = self.frame.group_by(
            ['Int32_0_15', 'Int32_0_31'], {'Int32_0_31': self.aggs})
        self._validate(stats, 'Int32_0_31', ['Int32_0_15', 'Int32_0_31'])

    def test_group_float32_multiple_cols(self):
        """Test groupby on multiple columns, float32"""
        stats = self.frame.group_by(
            ['Float32_0_15', 'Float32_0_31'], {'Float32_0_31': self.aggs})
        self._validate(stats, 'Float32_0_31', ['Float32_0_15', 'Float32_0_31'])

    def test_group_float64_multiple_cols(self):
        """Test groupby on multiple columns, float64"""
        stats = self.frame.group_by(
            ['Float64_0_15', 'Float64_0_31'], {'Float32_0_31': self.aggs})
        self._validate(stats, 'Float32_0_31', ['Float64_0_15', 'Float64_0_31'])

    def test_group_int64_multiple_cols(self):
        """Test groupby on multiple columns, int64"""
        stats = self.frame.group_by(
            ['Int64_0_15', 'Int64_0_31'], {'Int64_0_31': self.aggs})
        self._validate(stats, 'Int64_0_31',  ['Int64_0_15', 'Int64_0_31'])

    def test_groupby_str_multiple_cols(self):
        """Test groupby on multiple columns, string"""
        stats = self.frame.group_by(
            ['colors', 'Int32_0_15'], {'colors': self.aggs_str})
        self._validate_str(stats, 'colors',  ['colors', 'Int32_0_15'])

    def test_group_int32_none(self):
        """Test groupby none, int32 aggregate"""
        stats = self.frame.group_by(None, {'Int32_0_31': self.aggs})
        self._validate_single_group(stats, None, 'Int32_0_31')

    def test_group_float32_none(self):
        """Test groupby none, float32 aggregate"""
        stats = self.frame.group_by(None, {'Float32_0_31': self.aggs})
        self._validate_single_group(stats, None, 'Float32_0_31')

    def test_group_float64_none(self):
        """Test groupby none, float64 aggregate"""
        stats = self.frame.group_by(None, {'Float64_0_31': self.aggs})
        self._validate_single_group(stats, None, 'Float64_0_31')

    def test_group_int64_none(self):
        """Test groupby none, int64 aggregate"""
        stats = self.frame.group_by(None, {'Int64_0_31': self.aggs})
        self._validate_single_group(stats, None, 'Int64_0_31')

    def _validate_single_group(self, stats, groupby_cols, aggregator):
        # Validate the result of atk groupby and pandas groupby are the same
        # when there is  single group (none)
        pd_stats = stats.download(stats.count())
        new_frame = self.frame.download(self.frame.count())
        gb = new_frame.groupby(lambda x: 0)[aggregator].agg(self.numpy_aggs)
        int_cols = map(lambda x: aggregator+x, self.atk_cols)
        for k, l in zip(int_cols, self.pd_cols):
            self.assertAlmostEqual(gb.loc[0][l], pd_stats.loc[0][k], places=4)

    def _validate(self, stats, aggregator, groupby_cols):
        # Validate atk and pandas groupby are the same,
        # Cast the index to integer, and use all aggregates, as column
        # for aggregatees is numeric
        self._validate_helper(
            stats, aggregator, groupby_cols, self.numpy_aggs,
            self.pd_cols, self.atk_cols, int)

    def _validate_str(self, stats, aggregator, groupby_cols):
        # Validate atk and pandas groupby are the same,
        # Cast the index to the same value, and use strin aggregates, as column
        # for aggregatees is a string
        self._validate_helper(
            stats, aggregator, groupby_cols, self.numpy_aggs_str,
            self.pd_cols_str, self.atk_cols_str, lambda x: x)

    def _validate_helper(self, stats, aggregator, groupby_cols,
                         aggs, pd_cols, atk_cols, mapper):
        # Get and compare results of atk and pandas, cast as appropriate
        pd_stats = stats.download(stats.count())
        new_frame = self.frame.download(self.frame.count())
        gb = new_frame.groupby(groupby_cols)[aggregator].agg(aggs)
        int_cols = map(lambda x: aggregator+x, atk_cols)
        for _, i in pd_stats.iterrows():
            for k, l in zip(int_cols, pd_cols):
                if ((type(i[k]) is np.float64 or type(i[k]) is float) and
                   math.isnan(i[k])):
                    self.assertTrue(
                        math.isnan(
                            gb.loc[tuple(
                                map(lambda x: mapper(i[x]),
                                    groupby_cols))][l]))
                else:
                    self.assertAlmostEqual(
                        gb.loc[tuple(
                            map(lambda x: mapper(i[x]), groupby_cols))][l],
                        i[k], places=4)


if __name__ == "__main__":
    unittest.main()
