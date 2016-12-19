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

from sparktk.loggers import log_load; log_load(__name__); del log_load

from sparktk.propobj import PropertiesObject
from sparktk.models.regression.regression_test_metrics import RegressionTestMetrics
from sparktk import TkContext



__all__ = ["train", "load", "LinearRegressionModel"]

def train(frame,
          value_column,
          observation_columns,
          elastic_net_parameter=0.0,
          fit_intercept=True,
          max_iterations=100,
          reg_param=0.0,
          standardization=True,
          convergence_tolerance=1E-6):
    """
    Creates a LinearRegressionModel by training on the given frame

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
    :param convergence_tolerance: (str) Parameter for the convergence tolerance for iterative algorithms. Default is 1E-6
    :return: (LinearRegressionModel) A trained linear regression model
    """
    if frame is None:
        raise ValueError("frame cannot be None")

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
                                   convergence_tolerance)
    return LinearRegressionModel(tc, scala_model)


def load(path, tc=TkContext.implicit):
    """load LinearRegressionModel from given path"""
    TkContext.validate(tc)
    return tc.load(path, LinearRegressionModel)

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

        Consider the following frame with two columns.

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

        >>> model = tc.models.regression.linear_regression.train(frame,'y',['x1'])
        <progress>

        >>> model
        explained_variance_score= 0.987374330661
        intercept               = -0.0327272727273
        iterations              = 1
        mean_absolute_error     = 0.529939393939
        mean_squared_error      = 0.630096969697
        objective_history       = [0.0]
        observation_columns     = [u'x1']
        r2                      = 0.987374330661
        root_mean_squared_error = 0.793786476136
        value_column            = y
        weights                 = [2.4439393939393925]

        >>> linear_regression_test_return = model.test(frame, 'y')
        <progress>

        >>> linear_regression_test_return
        explained_variance_score= 0.987374330661
        mean_absolute_error     = 0.529939393939
        mean_squared_error      = 0.630096969697
        r2                      = 0.987374330661
        root_mean_squared_error = 0.793786476136

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

        >>> model.save("sandbox/linear_regression_model")

        >>> restored = tc.load("sandbox/linear_regression_model")

        >>> restored.value_column == model.value_column
        True

        >>> restored.intercept == model.intercept
        True

        >>> set(restored.observation_columns) == set(model.observation_columns)
        True

        >>> restored.test(frame, 'y').r2
        0.987374330660537

    The trained model can also be exported to a .mar file, to be used with the scoring engine:

        >>> canonical_path = model.export_to_mar("sandbox/linearRegressionModel.mar")

    <hide>
        >>> import os
        >>> assert(os.path.isfile(canonical_path))
    </hide>

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
        return self._tc.jutils.convert.from_scala_seq(self._scala.observationColumnsTrain())

    @property
    def intercept(self):
        """The intercept of the trained model"""
        return self._scala.intercept()

    @property
    def weights(self):
        """Weights of the trained model"""
        return self._tc.jutils.convert.from_scala_seq(self._scala.weights())

    @property
    def explained_variance_score(self):
        """The explained variance regression score whose best possible score is 1"""
        return self._scala.explainedVarianceScore()

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

        Parameters
        ----------

        :param frame: (Frame) The frame to predict on
        :param observation_columns: Optional(List[str]) List of column(s) containing the observations
        :return: (Frame) returns frame with predicted column added
        """
        from sparktk.frame.frame import Frame
        return Frame(self._tc, self._scala.predict(frame._scala, self._tc.jutils.convert.to_scala_option_list_string(observation_columns)))

    def test(self, frame, value_column, observation_columns=None):
        """
        Test the frame given the trained model

        Parameters
        ----------

        :param frame: (Frame) The frame to predict on
        :param value_column: (String) Column name containing the value for each observation
        :param observation_columns: Optional(List[str]) List of column(s) containing the observations
        :return: (RegressionTestMetrics) RegressionTestMetrics object consisting of results from model test
        """
        obs = self._tc.jutils.convert.to_scala_option_list_string(observation_columns)
        return RegressionTestMetrics(self._scala.test(frame._scala, value_column, obs))

    def save(self, path):
        """
        Saves the model to given path

        Parameters
        ----------

        :param path: (str) path to save
        """

        self._scala.save(self._tc._scala_sc, path, False)

    def export_to_mar(self, path):
        """
        Exports the trained model as a model archive (.mar) to the specified path.

        Parameters
        ----------

        :param path: (str) Path to save the trained model
        :return: (str) Full path to the saved .mar file

        """

        if not isinstance(path, basestring):
            raise TypeError("path parameter must be a str, but received %s" % type(path))

        return self._scala.exportToMar(self._tc._scala_sc, path)
