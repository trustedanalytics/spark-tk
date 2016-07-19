from sparktk.propobj import PropertiesObject


class LogisticRegressionSummaryTable(PropertiesObject):
    """
    LogisticRegressionSummaryTable holds the data returned from LogisticRegressionModel
    """
    def __init__(self, tc,  scala_result):
        self._tc = tc
        self._num_features = scala_result.numFeatures()
        self._num_classes = scala_result.numClasses()
        self._coefficients = self._tc.jutils.convert.scala_map_to_python(scala_result.coefficients())
        self._degrees_freedom = self._tc.jutils.convert.scala_map_to_python(scala_result.degreesFreedom())

        scala_option_frame = self._tc.jutils.convert.from_scala_option(scala_result.covarianceMatrix())
        if scala_option_frame:
            self._covariance_matrix = self._tc.frame.create(scala_option_frame)
        self._covariance_matrix = None

        scala_option_map = self._tc.jutils.convert.from_scala_option(scala_result.standardErrors())
        if scala_option_map:
            self._standard_errors = self._tc.jutils.convert.scala_map_to_python(scala_option_map)
        self._standard_errors = None

        scala_option_map = self._tc.jutils.convert.from_scala_option(scala_result.waldStatistic())
        if scala_option_map:
            self._wald_statistic = self._tc.jutils.convert.scala_map_to_python(scala_option_map)
        self._wald_statistic = None

        scala_option_map = self._tc.jutils.convert.from_scala_option(scala_result.pValue())
        if scala_option_map:
            self._p_value = self._tc.jutils.convert.scala_map_to_python(scala_option_map)
        self._p_value = None

    @property
    def num_features(self):
        """Number of features"""
        return self._num_features

    @property
    def num_classes(self):
        """Number of classes"""
        return self._num_classes

    @property
    def coefficients(self):
        """Model coefficients"""
        return self._coefficients

    @property
    def degrees_freedom(self):
        """Degrees of freedom for model coefficients"""
        return self._degrees_freedom

    @property
    def covariance_matrix(self):
        """Optional covariance matrix"""
        return self._covariance_matrix

    @property
    def standard_errors(self):
        """Optional standard errors for model coefficients"""
        return self._standard_errors

    @property
    def wald_statistic(self):
        """Optional Wald Chi-Squared statistic"""
        return self._wald_statistic

    @property
    def p_value(self):
        """Optional p-values for the model coefficients"""
        return self._p_value