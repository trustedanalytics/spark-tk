from sparktk.propobj import PropertiesObject

class ColumnSummaryStatistics(PropertiesObject):
    """
    ColumnSummaryStatistics class contains values that are returned from the column_summary_statistics frame operation.
    """
    def __init__(self, scala_result):
        self._mean = scala_result.mean()
        self._geometric_mean = scala_result.geometricMean()
        self._variance = scala_result.variance()
        self._standard_deviation = scala_result.standardDeviation()
        self._total_weight = scala_result.totalWeight()
        self._minimum = scala_result.minimum()
        self._maximum = scala_result.maximum()
        self._mean_confidence_lower = scala_result.meanConfidenceLower()
        self._mean_confidence_upper = scala_result.meanConfidenceUpper()
        self._bad_row_count = scala_result.badRowCount()
        self._good_row_count = scala_result.goodRowCount()
        self._positive_weight_count = scala_result.positiveWeightCount()
        self._non_positive_weight_count = scala_result.nonPositiveWeightCount()

    @property
    def mean(self):
        return self._mean

    @property
    def geometric_mean(self):
        return self._geometric_mean

    @property
    def variance(self):
        return self._variance

    @property
    def standard_deviation(self):
        return self._standard_deviation

    @property
    def total_weight(self):
        return self._total_weight

    @property
    def minimum(self):
        return self._minimum

    @property
    def maximum(self):
        return self._maximum

    @property
    def mean_confidence_lower(self):
        return self._mean_confidence_lower

    @property
    def mean_confidence_upper(self):
        return self._mean_confidence_upper

    @property
    def bad_row_count(self):
        return self._bad_row_count

    @property
    def good_row_count(self):
        return self._good_row_count

    @property
    def positive_weight_count(self):
        return self._positive_weight_count

    @property
    def non_positive_weight_count(self):
        return self._non_positive_weight_count

def column_summary_statistics(self, data_column, weights_column=None, use_popultion_variance=False):
    """
    Calculate multiple statistics for a column.

    :param data_column: The column to be statistically summarized.
                        Must contain numerical data; all NaNs and infinite values are excluded from the calculation.
    :param weights_column: Name of column holding weights of column values.
    :param use_popultion_variance: If true, the variance is calculated as the population variance.
                                   If false, the variance calculated as the sample variance.
                                   Because this option affects the variance, it affects the standard deviation and
                                   the confidence intervals as well.
                                   Default is false.
    :return: ColumnSummaryStatistics object containing summary statistics.

                The data returned is composed of multiple components\:

                |   mean : [ double | None ]
                |       Arithmetic mean of the data.
                |   geometric_mean : [ double | None ]
                |       Geometric mean of the data. None when there is a data element <= 0, 1.0 when there are no data elements.
                |   variance : [ double | None ]
                |       None when there are <= 1 many data elements. Sample variance is the weighted sum of the squared distance of each data element from the weighted mean, divided by the total weight minus 1. None when the sum of the weights is <= 1. Population variance is the weighted sum of the squared distance of each data element from the weighted mean, divided by the total weight.
                |   standard_deviation : [ double | None ]
                |       The square root of the variance. None when  sample variance is being used and the sum of weights is <= 1.
                |   total_weight : long
                |       The count of all data elements that are finite numbers. In other words, after excluding NaNs and infinite values.
                |   minimum : [ double | None ]
                |       Minimum value in the data. None when there are no data elements.
                |   maximum : [ double | None ]
                |       Maximum value in the data. None when there are no data elements.
                |   mean_confidence_lower : [ double | None ]
                |       Lower limit of the 95% confidence interval about the mean. Assumes a Gaussian distribution. None when there are no elements of positive weight.
                |   mean_confidence_upper : [ double | None ]
                |       Upper limit of the 95% confidence interval about the mean. Assumes a Gaussian distribution. None when there are no elements of positive weight.
                |   bad_row_count : [ double | None ]
                |       The number of rows containing a NaN or infinite value in either the data or weights column.
                |   good_row_count : [ double | None ]
                |       The number of rows not containing a NaN or infinite value in either the data or weights column.
                |   positive_weight_count : [ double | None ]
                |       The number of valid data elements with weight > 0. This is the number of entries used in the statistical calculation.
                |   non_positive_weight_count : [ double | None ]
                |       The number valid data elements with finite weight <= 0.

    Notes
    -----
    Sample Variance
        Sample Variance is computed by the following formula:

        .. math::

            \left( \frac{1}{W - 1} \right) * sum_{i} \
            \left(x_{i} - M \right) ^{2}

        where :math:`W` is sum of weights over valid elements of positive
        weight, and :math:`M` is the weighted mean.

    Population Variance
        Population Variance is computed by the following formula:

        .. math::

            \left( \frac{1}{W} \right) * sum_{i} \
            \left(x_{i} - M \right) ^{2}

        where :math:`W` is sum of weights over valid elements of positive
        weight, and :math:`M` is the weighted mean.

    Standard Deviation
        The square root of the variance.

    Logging Invalid Data
        A row is bad when it contains a NaN or infinite value in either
        its data or weights column.
        In this case, it contributes to bad_row_count; otherwise it
        contributes to good row count.

        A good row can be skipped because the value in its weight
        column is less than or equal to 0.
        In this case, it contributes to non_positive_weight_count, otherwise
        (when the weight is greater than 0) it contributes to
        valid_data_weight_pair_count.

    **Equations**

        .. code::

            bad_row_count + good_row_count = # rows in the frame
            positive_weight_count + non_positive_weight_count = good_row_count

        In particular, when no weights column is provided and all weights are 1.0:

        .. code::

            non_positive_weight_count = 0 and
            positive_weight_count = good_row_count


    Examples
    --------
    Given a frame with column 'a' accessed by a Frame object 'my_frame':

    .. code::

       >>> data = [[2],[3],[3],[5],[7],[10],[30]]
       >>> schema = [('a', int)]
       >>> my_frame = tc.frame.create(data, schema)
       <progress>

    Inspect my_frame

    .. code::

       >>> my_frame.inspect()
       [#]  a
       =======
       [0]   2
       [1]   3
       [2]   3
       [3]   5
       [4]   7
       [5]  10
       [6]  30

    Compute and return summary statistics for values in column *a*:

    .. code::

        >>> summary_statistics = my_frame.column_summary_statistics('a')
        <progress>
        >>> print summary_statistics
        bad_row_count             = 0
        geometric_mean            = 5.67257514519
        good_row_count            = 7
        maximum                   = 30.0
        mean                      = 8.57142857143
        mean_confidence_lower     = 1.27708372993
        mean_confidence_upper     = 15.8657734129
        minimum                   = 2.0
        non_positive_weight_count = 0
        positive_weight_count     = 7
        standard_deviation        = 9.84644001416
        total_weight              = 7.0
        variance                  = 96.9523809524

    Given a frame with column 'a' and column 'w' as weights accessed by a Frame object 'my_frame':

    .. code::

        >>> data = [[2,1.7],[3,0.5],[3,1.2],[5,0.8],[7,1.1],[10,0.8],[30,0.1]]
        >>> schema = [('a', int), ('w', float)]
        >>> my_frame = tc.frame.create(data, schema)
        <progress>

    Inspect my_frame

    .. code::

        >>> my_frame.inspect()
        [#]  a   w
        ============
        [0]   2  1.7
        [1]   3  0.5
        [2]   3  1.2
        [3]   5  0.8
        [4]   7  1.1
        [5]  10  0.8
        [6]  30  0.1

    Compute and return summary statistics values in column 'a' with weights 'w':

    .. code::
        >>> summary_statistics = my_frame.column_summary_statistics('a', weights_column='w')
        <progress>
        >>> print summary_statistics
        bad_row_count             = 0
        geometric_mean            = 4.03968288152
        good_row_count            = 7
        maximum                   = 30.0
        mean                      = 5.03225806452
        mean_confidence_lower     = 1.42847242276
        mean_confidence_upper     = 8.63604370627
        minimum                   = 2.0
        non_positive_weight_count = 0
        positive_weight_count     = 7
        standard_deviation        = 4.57824177679
        total_weight              = 6.2
        variance                  = 20.9602977667

    """
    return ColumnSummaryStatistics(self._scala.columnSummaryStatistics(data_column,
                                                                       self._tc.jutils.convert.to_scala_option(weights_column),
                                                                       use_popultion_variance))