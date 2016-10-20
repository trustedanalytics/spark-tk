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

import json


class AggregationFunctions(object):
    """
    Defines supported aggregation functions, maps them to keyword strings
    """
    avg = 'AVG'
    count = 'COUNT'
    count_distinct = 'COUNT_DISTINCT'
    max = 'MAX'
    min = 'MIN'
    sum = 'SUM'
    var = 'VAR'
    stdev = 'STDEV'

    def histogram(self, cutoffs, include_lowest=True, strict_binning=False):
        return repr(GroupByHistogram(cutoffs, include_lowest, strict_binning))

    def __repr__(self):
        return ", ".join([k for k in AggregationFunctions.__dict__.keys()
                          if isinstance(k, basestring) and not k.startswith("__")])

    def __contains__(self, item):
        return (item in AggregationFunctions.__dict__.values())


agg = AggregationFunctions()


class GroupByHistogram:
    """
    Class for histogram aggregation function that uses cutoffs to compute histograms
    """

    def __init__(self, cutoffs, include_lowest=True, strict_binning=False):
        for c in cutoffs:
            if not isinstance(c, (int, long, float, complex)):
                raise ValueError("Bad value %s in cutoffs, expected a number")
        self.cutoffs = cutoffs
        self.include_lowest = include_lowest
        self.strict_binning = strict_binning

    def __repr__(self):
        return 'HISTOGRAM=' + json.dumps(self.__dict__)


def group_by(self, group_by_columns, *aggregations):
    """
    Create a summarized frame with aggregations (Avg, Count, Max, Min, Mean, Sum, Stdev, ...).

    Parameters
    ----------

    :param group_by_columns: (List[str]) list of columns to group on
    :param aggregations: (dict) Aggregation function based on entire row, and/or dictionaries (one or more) of { column name str : aggregation function(s) }.
    :return: (Frame) Summarized Frame

    Creates a new frame and returns a Frame object to access it.Takes a column or group of columns, finds the unique combination of
    values, and creates unique rows with these column values.The other columns are combined according to the aggregation argument(s).

    Aggregation currently supports using the following functions:

            *   avg
            *   count
            *   count_distinct
            *   max
            *   min
            *   stdev
            *   sum
            *   var
            *   histogram()



    Notes
    -----
    *   Column order is not guaranteed when columns are added
    *   The column names created by aggregation functions in the new frame are the original column name appended
        with the '_' character and the aggregation function. For example, if the original field is *a* and the
        function is *avg*, the resultant column is named *a_avg*.

    *   An aggregation argument of *count* results in a column named *count*.
    *   The aggregation function *agg.count* is the only full row aggregation function supported at this time.

    Examples
    -------

    Consider this frame:

        <hide>
        >>> data = [[1, "alpha", 3.0, "small", 1, 3.0, 9],
        ...        [1, "bravo", 5.0, "medium", 1, 4.0, 9],
        ...        [1, "alpha", 5.0, "large", 1, 8.0, 8],
        ...        [2, "bravo", 8.0, "large", 1, 5.0, 7],
        ...        [2, "charlie", 12.0, "medium", 1, 6.0, 6],
        ...        [2, "bravo", 7.0, "small", 1, 8.0, 5],
        ...        [2, "bravo", 12.0, "large",  1, 6.0, 4]]
        >>> schema = [("a",int), ("b",str), ("c",float), ("d",str), ("e", int), ("f", float), ("g", int)]
        >>> frame = tc.frame.create(data, schema)
        <progress>
        </hide>

        >>> frame.inspect()
        [#]  a  b        c     d       e  f    g
        ========================================
        [0]  1  alpha     3.0  small   1  3.0  9
        [1]  1  bravo     5.0  medium  1  4.0  9
        [2]  1  alpha     5.0  large   1  8.0  8
        [3]  2  bravo     8.0  large   1  5.0  7
        [4]  2  charlie  12.0  medium  1  6.0  6
        [5]  2  bravo     7.0  small   1  8.0  5
        [6]  2  bravo    12.0  large   1  6.0  4

    Count the groups in column 'b'

        >>> b_count = frame.group_by('b', tc.agg.count)
        <progress>
        >>> b_count.inspect()
        [#]  b        count
        ===================
        [0]  alpha        2
        [1]  charlie      1
        [2]  bravo        4

    Group by columns 'a' and 'b' and compute the average for column 'c'

        >>> avg1 = frame.group_by(['a', 'b'], {'c' : tc.agg.avg})

        >>> avg1.inspect()
        [#]  a  b        c_AVG
        ======================
        [0]  2  charlie   12.0
        [1]  2  bravo      9.0
        [2]  1  bravo      5.0
        [3]  1  alpha      4.0

    Group by column 'a' and make a bunch of calculations for the grouped columns 'f' and 'g'

        >>> mix_frame = frame.group_by('a', tc.agg.count, {'f': [tc.agg.avg, tc.agg.sum, tc.agg.min], 'g': tc.agg.max})

        >>> mix_frame.inspect()
        [#]  a  count  g_MAX  f_AVG  f_SUM  f_MIN
        =========================================
        [0]  2      4      7   6.25   25.0    5.0
        [1]  1      3      9    5.0   15.0    3.0


    **Group by with histogram**.  The histogram aggregation argument is configured with these parameters:

    :param cutoffs: (List[int or float or long or double]) An array of values containing bin cutoff points.
    Array can be list or tuple. If an array is provided, values must be progressively increasing. All bin
    boundaries must be included, so, with N bins, you need N+1 values.  For example,

        cutoffs=[1, 5, 8, 12] # creates three bins:
                              #  bin0 holds values [1 inclusive - 5 exclusive]
                              #  bin1 holds values [5 inclusive - 8 exclusive]
                              #  bin2 holds values [8 inclusive - 9 exclusive]

    :param include_lowest: (Optional[bool]) Specify how the boundary conditions are handled. ``True``
    indicates that the lower bound of the bin is inclusive.  ``False`` indicates that the upper bound is
    inclusive. Default is ``True``.

    :param strict_binning: (Optional(bool)) Specify how values outside of the cutoffs array should be
    binned. If set to ``True``, each value less than cutoffs[0] or greater than cutoffs[-1] will be
    assigned a bin value of -1. If set to ``False``, values less than cutoffs[0] will be included in
    the first bin while values greater than cutoffs[-1] will be included in the final bin.

    Example
    -------

        >>> hist = frame.group_by('a', {'g': tc.agg.histogram([1, 5, 8, 9])})

        >>> hist.inspect()
        [#]  a  g_HISTOGRAM
        =========================
        [0]  2  [0.25, 0.75, 0.0]
        [1]  1    [0.0, 0.0, 1.0]

        >>> hist = frame.group_by('a', {'g': tc.agg.histogram([1, 5, 8, 9], False)})

        >>> hist.inspect()
        [#]  a  g_HISTOGRAM
        =============================================
        [0]  2                        [0.5, 0.5, 0.0]
        [1]  1  [0.0, 0.333333333333, 0.666666666667]

    """
    if group_by_columns is None:
        group_by_columns = []
    elif isinstance(group_by_columns, basestring):
        group_by_columns = [group_by_columns]

    first_column_name = None
    aggregation_list = []  # aggregationFunction : String, columnName : String, newColumnName

    for arg in aggregations:
        if arg == agg.count:
            if not first_column_name:
                # only make this call once, since it goes to http - TODO, ultimately should be handled server-side
                first_column_name = self.column_names[0]
            aggregation_list.append(
                    {'function': agg.count, 'column_name': first_column_name, 'new_column_name': "count"})
        elif isinstance(arg, dict):
            for key, value in arg.iteritems():
                # leave the valid column check to the server
                if isinstance(value, list) or isinstance(value, tuple):
                    for item in value:
                        if item not in agg:
                            raise ValueError(
                                "%s is not a valid aggregation function, like agg.max.  Supported agg methods: %s" % (
                                item, agg))
                        aggregation_list.append(
                                {'function': item, 'column_name': key, 'new_column_name': "%s_%s" % (key, item)})
                else:
                    aggregation_list.append(
                            {'function': value, 'column_name': key, 'new_column_name': "%s_%s" % (key, value)})
        else:
            raise TypeError(
                "Bad type %s provided in aggregation arguments; expecting an aggregation function or a dictionary of column_name:[func]" % type(
                    arg))

    scala_group_by_aggregation_args = []
    for item in aggregation_list:
        scala_group_by_aggregation_args.append(self._tc.jutils.convert.to_scala_group_by_aggregation_args(item))
    from sparktk.frame.frame import Frame
    return Frame(self._tc, self._scala.groupBy(self._tc.jutils.convert.to_scala_list_string(group_by_columns),
                                               self._tc.jutils.convert.to_scala_list(scala_group_by_aggregation_args)))
