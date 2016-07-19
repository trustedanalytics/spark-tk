from sparktk.loggers import log_load; log_load(__name__); del log_load

from sparktk.propobj import PropertiesObject


def train(frame,
          value_column,
          observation_columns,
          elastic_net_parameter=0.0,
          fit_intercept=True,
          max_iterations=100,
          reg_param=0.0,
          standardization=True,
          tolerance=1E-6):
    """
    Creates a PcaModel by training on the given frame

    Parameters
    ----------

    :param frame: (Frame) A frame to train the model on
    :param value_column: (str) Column name containing the value for each observation.
    :param observation_columns: (List[str]) List of column(s) containing the observations.
    :param elastic_net_parameter: (double) Parameter for the ElasticNet mixing. Default is 0.0
    :param fit_intercept: (bool) Parameter for whether to fit an intercept term. Default is true
    :param max_iterations: (int) Parameter for maximum number of iterations. Default is 100
    :param reg_param: (double) Parameter for regularization. Default is 0.0
    :param standardization: (bool) Parameter for whether to standardize the training features before fitting the model. Default is true
    :param tolerance: (str) Parameter for the convergence tolerance for iterative algorithms. Default is 1E-6
    :return: (LinearRegressionModel) A trained linear regression model
    """

    tc = frame._tc
    _scala_obj = get_scala_obj(tc)
    scala_observation_columns = tc.jutils.convert.to_scala_vector_string(observation_columns)
    if not isinstance(fit_intercept, bool):
        raise ValueError("fit_intercept must be a bool, received %s" % type(fit_intercept))
    if not isinstance(standardization, bool):
        raise ValueError("standardization must be a bool, received %s" % type(standardization))
    scala_model = _scala_obj.train(frame._scala,
                                   value_column,
                                   scala_observation_columns,
                                   elastic_net_parameter,
                                   fit_intercept,
                                   max_iterations,
                                   reg_param,
                                   standardization,
                                   tolerance)
    return LinearRegressionModel(tc, scala_model)


def get_scala_obj(tc):
    """Gets reference to the scala object"""
    return tc.sc._jvm.org.trustedanalytics.sparktk.models.regression.linear_regression.LinearRegressionModel


class LinearRegressionModel(PropertiesObject):
    """
    Linear Regression Model

    Example
    -------
        >>> rows = [[0,0],[1, 2.5],[2, 5.0],[3, 7.5],[4, 10],[5, 12.5],[6, 13.0],[7, 17.15], [8, 18.5],[9, 23.5]]
        >>> schema = [("x1", float),("y", float)]
        >>> frame = tc.frame.create(rows, schema)

        Consider the following frame containing two columns.

        >>> frame.inspect()
        [#]  x1  y
        ==============
        [0]   0      0
        [1]   1    2.5
        [2]   2    5.0
        [3]   3    7.5
        [4]   4     10
        [5]   5   12.5
        [6]   6   13.0
        [7]   7  17.15
        [8]   8   18.5
        [9]   9   23.5

        >>> model = tc.models.regression.linear_regression_model.train(frame,'y',['x1'])
        <progress>

        >>> model
        explained_variance      = 49.2759280303
        intercept               = -0.0327272727273
        iterations              = 3
        mean_absolute_error     = 0.529939393939
        mean_squared_error      = 0.630096969697
        objective_history       = [0.5, 0.007324606455391056, 0.006312834669731454]
        observation_columns     = [u'x1']
        r2                      = 0.987374330661
        root_mean_squared_error = 0.793786476136
        value_column            = y
        weights                 = WrappedArray(2.4439393939393934)

        >>> predicted_frame = model.predict(frame, ["x1"])
        <progress>

        >>> predicted_frame.inspect()
        [#]  x1   y      predicted_value
        =================================
        [0]  0.0    0.0  -0.0327272727273
        [1]  1.0    2.5     2.41121212121
        [2]  2.0    5.0     4.85515151515
        [3]  3.0    7.5     7.29909090909
        [4]  4.0   10.0     9.74303030303
        [5]  5.0   12.5      12.186969697
        [6]  6.0   13.0     14.6309090909
        [7]  7.0  17.15     17.0748484848
        [8]  8.0   18.5     19.5187878788
        [9]  9.0   23.5     21.9627272727

        """

    def __init__(self, tc, scala_model):
        self._tc = tc
        tc.jutils.validate_is_jvm_instance_of(scala_model, get_scala_obj(tc))
        self._scala = scala_model

    @staticmethod
    def _from_scala(tc, scala_model):
        return LinearRegressionModel(tc, scala_model)

    @property
    def value_column(self):
        """Column name containing the value for each observation."""
        return self._scala.valueColumn()

    @property
    def observation_columns(self):
        """List of column(s) containing the observations."""
        return self._tc.jutils.convert.from_scala_vector(self._scala.observationColumnsTrain())

    @property
    def intercept(self):
        """The intercept of the trained model"""
        return self._scala.intercept()

    @property
    def weights(self):
        """Weights of the trained model"""
        return self._scala.weights()

    @property
    def explained_variance(self):
        """The explained variance regression score"""
        return self._scala.explainedVariance()

    @property
    def mean_absolute_error(self):
        """The risk function corresponding to the expected value of the absolute error loss or l1-norm loss"""
        return self._scala.meanAbsoluteError()

    @property
    def mean_squared_error(self):
        """The risk function corresponding to the expected value of the squared error loss or quadratic loss"""
        return self._scala.meanSquaredError()

    @property
    def objective_history(self):
        """Objective function(scaled loss + regularization) at each iteration"""
        return self._tc.jutils.convert.from_scala_seq(self._scala.objectiveHistory())

    @property
    def r2(self):
        """The coefficient of determination of the trained model"""
        return self._scala.r2()

    @property
    def root_mean_squared_error(self):
        """The square root of the mean squared error"""
        return self._scala.rootMeanSquaredError()

    @property
    def iterations(self):
        """The number of training iterations until termination"""
        return self._scala.iterations()

    def predict(self, frame, observation_columns):
        """
        Predict values for a frame using a trained Linear Regression model

        :param frame: (Frame) The frame to predict on
        :param observation_columns: (List[str]) List of column(s) containing the observations
        :return: (Frame) returns predicted frame
        """
        return self._tc.frame.create(self._scala.predict(frame._scala, self._tc.jutils.convert.to_scala_option_list_string(observation_columns)))

    def save(self, path):
        """
        Saves the model to given path

        :param path: (str) path to save
        """
        self._scala.save(self._tc._scala_sc, path)