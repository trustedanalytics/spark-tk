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

from sparktk.propobj import PropertiesObject
from sparktk.frame.ops.inspect import ATable


class CategoricalSummaryOutputList(list):
    def __str__(self):
        return "\n\n".join([str(item) for item in self])

    def __repr__(self):
        return str(self)


class CategoricalSummaryOutput(PropertiesObject):
    """
    CategoricalSummaryOutput class containing the levels with their frequency and percentage for the specified column.
    """
    def __init__(self, scala_result):
        self._column_name = scala_result.column()
        self._levels = [LevelData(item) for item in list(scala_result.levels())]

    @property
    def column_name(self):
        return self._column_name

    @property
    def levels(self):
        return self._levels

    def __str__(self):
        rows = []
        for level_data in self.levels:
            rows.append([level_data.level, level_data.frequency, level_data.percentage])
        schema=[("level", str), ("frequency", int), ("percentage", float)]
        rows = ATable(rows, schema, 0)

        return "column_name = \"{0}\"\n{1}".format(self.column_name, rows)

    def __repr__(self):
        return str(self)


class LevelData(PropertiesObject):
    def __init__(self, scala_result):
        self._level = scala_result.level()
        self._frequency = scala_result.frequency()
        self._percentage = scala_result.percentage()

    @property
    def level(self):
        return self._level

    @property
    def frequency(self):
        return self._frequency

    @property
    def percentage(self):
        return self._percentage


def categorical_summary(self, columns, top_k=None, threshold=None):
    """
    Build summary of the data.

    Parameters
    ----------

    :param columns: (List[CategoricalSummaryInput]) List of CategoricalSummaryInput consisting of column, topk and/or threshold
    :param top_k: (Optional[int]) Displays levels which are in the top k most frequently
            occurring values for that column.
            Default is 10.
    :param threshold: (Optional[float]) Displays levels which are above the threshold percentage with
            respect to the total row count.
            Default is 0.0.
    :return: (List[CategoricalSummaryOutput]) List of CategoricalSummaryOutput objects for specified column(s) consisting of levels with
             their frequency and percentage.

    Compute a summary of the data in a column(s) for categorical or numerical data types.
    The returned value is a Map containing categorical summary for each specified column.

    For each column, levels which satisfy the top k and/or threshold cutoffs are
    displayed along with their frequency and percentage occurrence with respect to
    the total rows in the dataset.

    Performs level pruning first based on top k and then filters
    out levels which satisfy the threshold criterion.

    Missing data is reported when a column value is empty ("") or null.

    All remaining data is grouped together in the Other category and its frequency
    and percentage are reported as well.

    User must specify the column name and can optionally specify top_k and/or threshold.

    Examples
    --------

    Consider Frame *my_frame*, which contains the data

    <hide>
        >>> s = [("source",str),("target",str)]
        >>> rows = [ ["entity","thing"], ["entity","physical_entity"],["entity","abstraction"],["physical_entity","entity"],["physical_entity","matter"],["physical_entity","process"],["physical_entity","thing"],["physical_entity","substance"],["physical_entity","object"],["physical_entity","causal_agent"],["abstraction","entity"],["abstraction","communication"],["abstraction","group"],["abstraction","otherworld"],["abstraction","psychological_feature"],["abstraction","attribute"],["abstraction","set"],["abstraction","measure"],["abstraction","relation"],["thing","physical_entity"],["thing","reservoir"],["thing","part"],["thing","subject"],["thing","necessity"],["thing","variable"],["thing","unit"],["thing","inessential"],["thing","body_of_water"]]
        >>> my_frame = tc.frame.create(rows, s)
        -etc-
    </hide>

        >>> my_frame.inspect()
        [#]  source           target
        =====================================
        [0]  entity           thing
        [1]  entity           physical_entity
        [2]  entity           abstraction
        [3]  physical_entity  entity
        [4]  physical_entity  matter
        [5]  physical_entity  process
        [6]  physical_entity  thing
        [7]  physical_entity  substance
        [8]  physical_entity  object
        [9]  physical_entity  causal_agent

        >>> cm = my_frame.categorical_summary('source', top_k=2)
        <progress>

        >>> cm
        column_name = "source"
        [#]  level        frequency  percentage
        ===========================================
        [0]  thing                9  0.321428571429
        [1]  abstraction          9  0.321428571429
        [2]  <Missing>            0             0.0
        [3]  <Other>             10  0.357142857143

        >>> cm = my_frame.categorical_summary('source', threshold = 0.5)
        <progress>

        >>> cm
        column_name = "source"
        [#]  level      frequency  percentage
        =====================================
        [0]  <Missing>          0         0.0
        [1]  <Other>           28         1.0

        >>> cm = my_frame.categorical_summary(['source', 'target'], top_k=[2, None], threshold=[None, 0.5])
        <progress>

        >>> cm
        column_name = "source"
        [#]  level        frequency  percentage
        ===========================================
        [0]  thing                9  0.321428571429
        [1]  abstraction          9  0.321428571429
        [2]  <Missing>            0             0.0
        [3]  <Other>             10  0.357142857143
        <BLANKLINE>
        column_name = "target"
        [#]  level      frequency  percentage
        =====================================
        [0]  <Missing>          0         0.0
        [1]  <Other>           28         1.0

    """
    if not isinstance(columns, list):
        columns = [columns]
    columns = self._tc.jutils.convert.to_scala_list_string(columns)

    if top_k is not None:
        if not isinstance(top_k, list):
            top_k = [top_k]
        top_k = [self._tc.jutils.convert.to_scala_option(item) for item in top_k]
        top_k = self._tc.jutils.convert.to_scala_list(top_k)
    if threshold is not None:
        if not isinstance(threshold, list):
            threshold = [threshold]
        threshold = [self._tc.jutils.convert.to_scala_option(item) for item in threshold]
        threshold = self._tc.jutils.convert.to_scala_list(threshold)
    result_list = list(self._scala.categoricalSummary(columns,
                                                      self._tc.jutils.convert.to_scala_option(top_k),
                                                      self._tc.jutils.convert.to_scala_option(threshold)))
    return CategoricalSummaryOutputList([CategoricalSummaryOutput(item) for item in result_list])
