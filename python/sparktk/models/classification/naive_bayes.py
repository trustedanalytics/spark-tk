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
from sparktk.frame.ops.classification_metrics_value import ClassificationMetricsValue
from sparktk import TkContext
from sparktk.arguments import affirm_type

__all__ = ["train", "load", "NaiveBayesModel"]

def train(frame, label_column, observation_columns, lambda_parameter = 1.0):
    """
    Creates a Naive Bayes by training on the given frame

    :param frame: (Frame) frame of training data
    :param label_column: (str) Column containing the label for each observation
    :param observation_columns: (List[str]) Column(s) containing the observations
    :param lambda_parameter: (float) Additive smoothing parameter Default is 1.0

    :return: (NaiveBayesModel) Trained Naive Bayes model

    """
    if frame is None:
        raise ValueError("frame cannot be None")
    tc = frame._tc
    _scala_obj = get_scala_obj(tc)
    scala_model = _scala_obj.train(frame._scala,
                                   label_column,
                                   tc.jutils.convert.to_scala_list_string(observation_columns),
                                   lambda_parameter)
    return NaiveBayesModel(tc, scala_model)


def load(path, tc=TkContext.implicit):
    """load NaiveBayesModel from given path"""
    TkContext.validate(tc)
    return tc.load(path, NaiveBayesModel)


def get_scala_obj(tc):
    """Gets reference to the scala object"""
    return tc.sc._jvm.org.trustedanalytics.sparktk.models.classification.naive_bayes.NaiveBayesModel


class NaiveBayesModel(PropertiesObject):
    """
    A trained Naive Bayes model

    Example
    -------

        >>> frame = tc.frame.create([[1,19.8446136104,2.2985856384],
        ...                          [1,16.8973559126,2.6933495054],
        ...                          [1,5.5548729596, 2.7777687995],
        ...                          [0,46.1810010826,3.1611961917],
        ...                          [0,44.3117586448,3.3458963222],
        ...                          [0,34.6334526911,3.6429838715]],
        ...                          [('Class', int), ('Dim_1', float), ('Dim_2', float)])

        >>> model = tc.models.classification.naive_bayes.train(frame, 'Class', ['Dim_1', 'Dim_2'], 0.9)

        >>> model.label_column
        u'Class'

        >>> model.observation_columns
        [u'Dim_1', u'Dim_2']

        >>> model.lambda_parameter
        0.9

        >>> predicted_frame = model.predict(frame, ['Dim_1', 'Dim_2'])

        >>> predicted_frame.inspect()
        [#]  Class  Dim_1          Dim_2         predicted_class
        ========================================================
        [0]      1  19.8446136104  2.2985856384              0.0
        [1]      1  16.8973559126  2.6933495054              1.0
        [2]      1   5.5548729596  2.7777687995              1.0
        [3]      0  46.1810010826  3.1611961917              0.0
        [4]      0  44.3117586448  3.3458963222              0.0
        [5]      0  34.6334526911  3.6429838715              0.0

        >>> model.save("sandbox/naivebayes")

        >>> restored = tc.load("sandbox/naivebayes")

        >>> restored.label_column == model.label_column
        True

        >>> restored.lambda_parameter == model.lambda_parameter
        True

        >>> set(restored.observation_columns) == set(model.observation_columns)
        True

        >>> metrics = model.test(frame, "Class", ["Dim_1", "Dim_2"])

        >>> metrics.precision
        1.0

        >>> predicted_frame2 = restored.predict(frame, ['Dim_1', 'Dim_2'])

        >>> predicted_frame2.inspect()
        [#]  Class  Dim_1          Dim_2         predicted_class
        ========================================================
        [0]      1  19.8446136104  2.2985856384              0.0
        [1]      1  16.8973559126  2.6933495054              1.0
        [2]      1   5.5548729596  2.7777687995              1.0
        [3]      0  46.1810010826  3.1611961917              0.0
        [4]      0  44.3117586448  3.3458963222              0.0
        [5]      0  34.6334526911  3.6429838715              0.0


        >>> canonical_path = model.export_to_mar("sandbox/naivebayes.mar")

    <hide>
    >>> import os
    >>> os.path.exists(canonical_path)
    True
    </hide>

    """

    def __init__(self, tc, scala_model):
        self._tc = tc
        tc.jutils.validate_is_jvm_instance_of(scala_model, get_scala_obj(tc))
        self._scala = scala_model

    @staticmethod
    def _from_scala(tc, scala_model):
        return NaiveBayesModel(tc, scala_model)

    @property
    def label_column(self):
        return self._scala.labelColumnName()

    @property
    def observation_columns(self):
        return self._tc.jutils.convert.from_scala_seq(self._scala.observationColumnNames())

    @property
    def lambda_parameter(self):
        return self._scala.lambdaParameter()


    def predict(self, future_periods = 0, ts = None):
        """
        Forecasts future periods using ARIMA.

        Provided fitted values of the time series as 1-step ahead forecasts, based on current model parameters, then
        provide future periods of forecast.  We assume AR terms prior to the start of the series are equal to the
        model's intercept term (or 0.0, if fit without an intercept term).  Meanwhile, MA terms prior to the start
        are assumed to be 0.0.  If there is differencing, the first d terms come from the original series.

        :param future_periods: (int) Periods in the future to forecast (beyond length of time series that the
                               model was trained with).
        :param ts: (Optional(List[float])) Optional list of time series values to use as golden values.  If no time
                   series values are provided, the values used during training will be used during forecasting.

        """
        if not isinstance(future_periods, int):
            raise TypeError("'future_periods' parameter must be an integer.")
        if ts is not None:
            if not isinstance(ts, list):
                raise TypeError("'ts' parameter must be a list of float values." )

        ts_predict_values = self._tc.jutils.convert.to_scala_option_list_double(ts)

        return list(self._tc.jutils.convert.from_scala_seq(self._scala.predict(future_periods, ts_predict_values)))


    def predict(self, frame, observation_columns=None):
        """
        Predicts the labels for the observation columns in the given input frame. Creates a new frame
        with the existing columns and a new predicted column.

        Parameters
        ----------

        :param frame: (Frame) Frame used for predicting the values
        :param c: (List[str]) Names of the observation columns.
        :return: (Frame) A new frame containing the original frame's columns and a prediction column
        """
        c = self._tc.jutils.convert.to_scala_option_list_string(self.__get_observation_columns(observation_columns))
        from sparktk.frame.frame import Frame
        return Frame(self._tc, self._scala.predict(frame._scala, c))

    def test(self, frame, label_column, observation_columns=None):
        """test the frame given the trained model"""
        columns_list = self.__get_observation_columns(observation_columns)
        scala_classification_metrics_object = self._scala.test(frame._scala,
                                                               label_column,
                                                               self._tc.jutils.convert.to_scala_option_list_string(columns_list))
        return ClassificationMetricsValue(self._tc, scala_classification_metrics_object)

    def __get_observation_columns(self, observation_columns):
        if observation_columns is None:
            return observation_columns
        else:
            return affirm_type.list_of_str(observation_columns, "observation_columns")

    def __columns_to_option(self, c):
        if c is not None:
            c = self._tc.jutils.convert.to_scala_list_string(c)
        return self._tc.jutils.convert.to_scala_option(c)

    def save(self, path):
        self._scala.save(self._tc._scala_sc, path)

    def export_to_mar(self, path):
        """
        Exports the trained model as a model archive (.mar) to the specified path

        Parameters
        ----------

        :param path: (str) Path to save the trained model
        :return: (str) Full path to the saved .mar file
        """
        if isinstance(path, basestring):
            return self._scala.exportToMar(self._tc._scala_sc, path)

del PropertiesObject
