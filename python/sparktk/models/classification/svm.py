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

__all__ = ["train", "load", "SvmModel"]

def train(frame,
          label_column,
          observation_columns,
          intercept = True,
          num_iterations = 100,
          step_size = 1.0,
          reg_type = None,
          reg_param = 0.01,
          mini_batch_fraction = 1.0):
    """
    Creates a Svm Model by training on the given frame

    Parameters
    ----------

    :param frame: (Frame) frame of training data
    :param label_column: (str) Column containing the label for each observation
    :param observation_columns: (list(str)) Column(s) containing the observations
    :param intercept: (boolean) Flag indicating if the algorithm adds an intercept. Default is true
    :param num_iterations: (int) Number of iterations for SGD. Default is 100
    :param step_size: (float) Initial step size for SGD optimizer for the first step. Default is 1.0
    :param reg_type: (Optional(str)) Regularization "L1" or "L2". Default is "L2"
    :param reg_param: (float) Regularization parameter. Default is 0.01
    :param mini_batch_fraction: (float) Set fraction of data to be used for each SGD iteration. Default is 1.0; corresponding to deterministic/classical gradient descent
    :return: (SvmModel) The SVM trained model (with SGD)

    Notes
    -----
    Support Vector Machine is a supervised algorithm used to perform binary classification. A Support Vector Machine
    constructs a high dimensional hyperplane which is said to achieve a good separation when a hyperplane has the
    largest distance to the nearest training-data point of any class. This model runs the MLLib implementation of SVM
    with SGD optimizer. The SVM model is initialized, trained on columns of a frame, used to predict the labels
    of observations in a frame, and tests the predicted labels against the true labels. During testing, labels of the
    observations are predicted and tested against the true labels using built-in binary Classification Metrics.

    """
    tc = frame._tc
    _scala_obj = get_scala_obj(tc)
    scala_model = _scala_obj.train(frame._scala,
                                   label_column,
                                   tc.jutils.convert.to_scala_list_string(observation_columns),
                                   intercept,
                                   num_iterations,
                                   step_size,
                                   tc.jutils.convert.to_scala_option(reg_type),
                                   reg_param,
                                   mini_batch_fraction)

    return SvmModel(tc, scala_model)


def load(path, tc=TkContext.implicit):
    """load SvmModel from given path"""
    TkContext.validate(tc)
    return tc.load(path, SvmModel)


def get_scala_obj(tc):
    """Gets reference to the scala object"""
    return tc.sc._jvm.org.trustedanalytics.sparktk.models.classification.svm.SvmModel


class SvmModel(PropertiesObject):
    """
    A trained Svm model

    Example
    -------
        >>> frame = tc.frame.create([[-48.0,1], [-75.0,1], [-63.0,1], [-57.0,1], [73.0,0], [-33.0,1], [100.0,0],
        ...                          [-54.0,1], [78.0,0], [48.0,0], [-55.0,1], [23.0,0], [45.0,0], [75.0,0]],
        ...                          [("data", float),("label", str)])


        >>> frame.inspect()
        [#]  data   label
        =================
        [0]  -48.0  1
        [1]  -75.0  1
        [2]  -63.0  1
        [3]  -57.0  1
        [4]   73.0  0
        [5]  -33.0  1
        [6]  100.0  0
        [7]  -54.0  1
        [8]   78.0  0
        [9]   48.0  0

        >>> model = tc.models.classification.svm.train(frame, 'label', ['data'])

        >>> model.label_column
        u'label'

        >>> model.observation_columns
        [u'data']

        >>> model.predict(frame, ['data'])

        >>> frame.inspect()
        [#]  data   label  predicted_label
        ==================================
        [0]  -48.0  1                    1
        [1]  -75.0  1                    1
        [2]  -63.0  1                    1
        [3]  -57.0  1                    1
        [4]   73.0  0                    0
        [5]  -33.0  1                    1
        [6]  100.0  0                    0
        [7]  -54.0  1                    1
        [8]   78.0  0                    0
        [9]   48.0  0                    0


        >>> test_metrics = model.test(frame)

        >>> test_metrics
        accuracy         = 1.0
        confusion_matrix =             Predicted_Pos  Predicted_Neg
        Actual_Pos              7              0
        Actual_Neg              0              7
        f_measure        = 1.0
        precision        = 1.0
        recall           = 1.0

        >>> model.save("sandbox/svm")

        >>> restored = tc.load("sandbox/svm")

        >>> restored.label_column == model.label_column
        True

        >>> restored.intercept == model.intercept
        True

        >>> set(restored.observation_columns) == set(model.observation_columns)
        True

    """

    def __init__(self, tc, scala_model):
        self._tc = tc
        tc.jutils.validate_is_jvm_instance_of(scala_model, get_scala_obj(tc))
        self._scala = scala_model

    @staticmethod
    def _from_scala(tc, scala_model):
        return SvmModel(tc, scala_model)

    @property
    def label_column(self):
        """column containing the label used during model training"""
        return self._scala.labelColumn()

    @property
    def observation_columns(self):
        """columns containing the observation values used during model training"""
        return self._tc.jutils.convert.from_scala_seq(self._scala.observationColumns())

    @property
    def intercept(self):
        """intercept used during model training"""
        return self._scala.intercept()

    @property
    def num_iterations(self):
        """max number of iterations allowed during model training"""
        return self._scala.numIterations()

    @property
    def step_size(self):
        """step size value used to train the model"""
        return self._scala.stepSize()

    @property
    def reg_type(self):
        """regularization type used to train the model"""
        return self._tc.jutils.convert.from_scala_option(self._scala.regType())

    @property
    def reg_param(self):
        """regularization parameter used to train the model"""
        return self._scala.regParam()

    @property
    def mini_batch_fraction(self):
        """minimum batch fraction used to train the model"""
        return self._scala.miniBatchFraction()

    def predict(self, frame, columns=None):
        """predict the frame given the trained model"""
        c = self.__columns_to_option(columns)
        self._scala.predict(frame._scala, c)

    def test(self, frame, columns=None):
        """test the frame given the trained model"""
        c = self.__columns_to_option(columns)
        return ClassificationMetricsValue(self._tc, self._scala.test(frame._scala, c))

    def __columns_to_option(self, c):
        if c is not None:
            c = self._tc.jutils.convert.to_scala_list_string(c)
        return self._tc.jutils.convert.to_scala_option(c)

    def save(self, path):
        """save the trained model to path"""
        self._scala.save(self._tc._scala_sc, path)

del PropertiesObject
