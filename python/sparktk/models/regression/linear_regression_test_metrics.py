from sparktk.propobj import PropertiesObject

class LinearRegressionTestMetrics(PropertiesObject):
    """
    RegressionMetrics class used to hold the data returned from linear regression test
    """
    def __init__(self, scala_result):
        self._explained_variance = scala_result.explainedVariance()
        self._mean_absolute_error = scala_result.meanAbsoluteError()
        self._mean_squared_error = scala_result.meanSquaredError()
        self._r2 = scala_result.r2()
        self._root_mean_squared_error = scala_result.rootMeanSquaredError()

    @property
    def explained_variance(self):
        """The explained variance regression score"""
        return self._explained_variance

    @property
    def mean_absolute_error(self):
        """The risk function corresponding to the expected value of the absolute error loss or l1-norm loss"""
        return self._mean_absolute_error

    @property
    def mean_squared_error(self):
        """The risk function corresponding to the expected value of the squared error loss or quadratic loss"""
        return self._mean_squared_error

    @property
    def r2(self):
        """The coefficient of determination"""
        return self._r2

    @property
    def root_mean_squared_error(self):
        """The square root of the mean squared error"""
        return self._root_mean_squared_error
