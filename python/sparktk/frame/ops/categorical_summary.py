from sparktk.propobj import PropertiesObject

class CategoricalSummaryInput(PropertiesObject):
    """
    CategoricalSummaryInput class containing optional parameters for each column to categorial_summary
    """
    def __init__(self, column_name, top_k=None, threshold=None):
        if not isinstance(column_name, str):
            raise TypeError("Argument column_name in the CategoricalSummaryInput class must be a str, but was %s." % type(column_name))
        if top_k is not None and not isinstance(top_k, int):
            raise TypeError("Argument top_k in the CategoricalSummaryInput class must be an int, but was %s." % type(top_k))
        if threshold is not None and not isinstance(threshold, int) and not isinstance(threshold, float):
            raise TypeError("Argument threshold in the CategoricalSummaryInput class must be numerical, but was %s." % type(threshold))
        self._column_name = column_name
        self._top_k = top_k
        self._threshold = float(threshold) if threshold is not None else threshold

    @property
    def column_name(self):
        return self._column_name

    @property
    def top_k(self):
        return self._top_k

    @property
    def threshold(self):
        return self._threshold

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

def categorical_summary(self, *column_inputs):
    """
    Build summary of the data.

    :param column_inputs: List of CategoricalSummaryInput consisting of column, topk and/or threshold
    :return: List of CategoricalSummaryOutput objects for specified column(s) consisting of levels with
             their frequency and percentage.

    Optional parameters:

        top_k *: int*
            Displays levels which are in the top k most frequently
            occurring values for that column.
            Default is 10.

        threshold *: float*
            Displays levels which are above the threshold percentage with
            respect to the total row count.
            Default is 0.0.

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
    >>> my_frame = tc.to_frame(rows, s)
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

    >>> cm = my_frame.categorical_summary(tc.CategoricalSummaryInput('source', top_k=2))
    <progress>

    >>> cm
    [column_name = source
     levels      = [frequency  = 9
     level      = thing
     percentage = 0.321428571429, frequency  = 9
     level      = abstraction
     percentage = 0.321428571429, frequency  = 0
     level      = Missing
     percentage = 0.0, frequency  = 10
     level      = Other
     percentage = 0.357142857143]]

    >>> cm = my_frame.categorical_summary(tc.CategoricalSummaryInput('source', threshold = 0.5))
    <progress>

    >>> cm
    [column_name = source
     levels      = [frequency  = 0
     level      = Missing
     percentage = 0.0, frequency  = 28
     level      = Other
     percentage = 1.0]]

    >>> cm = my_frame.categorical_summary(tc.CategoricalSummaryInput('source', top_k=2), tc.CategoricalSummaryInput('target', threshold=0.5))
    <progress>

    >>> cm
    [column_name = source
     levels      = [frequency  = 9
     level      = thing
     percentage = 0.321428571429, frequency  = 9
     level      = abstraction
     percentage = 0.321428571429, frequency  = 0
     level      = Missing
     percentage = 0.0, frequency  = 10
     level      = Other
     percentage = 0.357142857143], column_name = target
     levels      = [frequency  = 0
     level      = Missing
     percentage = 0.0, frequency  = 28
     level      = Other
     percentage = 1.0]]

    """
    column_input_list = []
    for column_input in column_inputs:
        if not isinstance(column_input, CategoricalSummaryInput):
            raise TypeError("Argument 'column_inputs' must be of type CategoricalSummaryInput, but was %s." % type(column_input))
        column_input_list.append((unicode(column_input.column_name),
                                  self._tc.jutils.convert.to_scala_option(column_input.top_k),
                                  self._tc.jutils.convert.to_scala_option(column_input.threshold)))
    result_list = list(self._scala.categoricalSummary(self._tc.jutils.convert.to_scala_list_categorical_summary(column_input_list)))
    return [CategoricalSummaryOutput(item) for item in result_list]
