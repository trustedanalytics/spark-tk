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

def train(frame, observation_columns, label_column, lambda_parameter = 1.0):
    """
    Creates a Naive Bayes by training on the given frame

    :param frame: (Frame) frame of training data
    :param observation_columns: (List[str]) Column(s) containing the observations
    :param label_column: (str) Column containing the label for each observation
    :param lambda_parameter: (float) Additive smoothing parameter Default is 1.0

    :return: (NaiveBayesModel) Trained Naive Bayes model

    """
    if frame is None:
        raise ValueError("frame cannot be None")
    tc = frame._tc
    _scala_obj = get_scala_obj(tc)
    scala_model = _scala_obj.train(frame._scala,
                                   tc.jutils.convert.to_scala_list_string(observation_columns),
                                   label_column,
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

        >>> model = tc.models.classification.naive_bayes.train(frame, ['Dim_1', 'Dim_2'], 'Class', 0.9)

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

        >>> metrics = model.test(frame, ["Dim_1", "Dim_2"], "Class")

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
        return self._scala.labelColumn()

    @property
    def observation_columns(self):
        return self._tc.jutils.convert.from_scala_seq(self._scala.observationColumns())

    @property
    def lambda_parameter(self):
        return self._scala.lambdaParameter()

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

    def test(self, frame, observation_columns=None, label_column=None):
        """test the frame given the trained model"""
        columns_list = self.__get_observation_columns(observation_columns)
        scala_classification_metrics_object = self._scala.test(frame._scala,
                                                               self._tc.jutils.convert.to_scala_option_list_string(columns_list),
                                                               self._tc.jutils.convert.to_scala_option(label_column))
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
