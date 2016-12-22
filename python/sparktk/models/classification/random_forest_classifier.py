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
from sparktk.frame.frame import Frame
from sparktk.arguments import affirm_type
import os

__all__ = ["train", "load", "RandomForestClassifierModel"]

def train(frame,
          observation_columns,
          label_column,
          num_classes = 2,
          num_trees = 1,
          impurity = "gini",
          max_depth = 4,
          max_bins = 100,
          seed = None,
          categorical_features_info = None,
          feature_subset_category = None,
          min_instances_per_node = None,
          sub_sampling_rate = None
          ):
    """
    Creates a Random Forest Classifier Model by training on the given frame

    Parameters
    ----------

    :param frame: (Frame) frame frame of training data
    :param observation_columns: (list(str)) Column(s) containing the observations
    :param label_column: (str) Column name containing the label for each observation
    :param num_classes: (int) Number of classes for classification. Default is 2
    :param num_trees: (int) Number of tress in the random forest. Default is 1
    :param impurity: (str) Criterion used for information gain calculation. Supported values "gini" or "entropy".
                    Default is "gini"
    :param max_depth: (int) Maximum depth of the tree. Default is 4
    :param max_bins: (int) Maximum number of bins used for splitting features. Default is 100
    :param seed: (Optional(int)) Random seed for bootstrapping and choosing feature subsets. Default is a randomly chosen seed
    :param categorical_features_info: (Optional(Dict(str:int))) Arity of categorical features. Entry (name-> k) indicates
                                      that feature 'name' is categorical with 'k' categories indexed from 0:{0,1,...,k-1}
    :param feature_subset_category: (Optional(str)) Number of features to consider for splits at each node.
                                 Supported values "auto","all","sqrt","log2","onethird".
                                 If "auto" is set, this is based on num_trees: if num_trees == 1, set to "all"
                                 ; if num_trees > 1, set to "sqrt"
    :param min_instances_per_node: (Optional(int)) Minimum number of instances each child must have after split. Default is 1
    :param sub_sampling_rate: (Optional(double)) Fraction of the training data used for learning each decision tree. Default is 1.0

    :return: (RandomForestClassifierModel) The trained random forest classifier model

    Notes
    -----
    Random Forest is a supervised ensemble learning algorithm which can be used to perform binary and
    multi-class classification. The Random Forest Classifier model is initialized, trained on columns of a frame,
    used to predict the labels of observations in a frame, and tests the predicted labels against the true labels.
    This model runs the MLLib implementation of Random Forest. During training, the decision trees are trained
    in parallel. During prediction, each tree's prediction is counted as vote for one class. The label is predicted
    to be the class which receives the most votes. During testing, labels of the observations are predicted and
    tested against the true labels using built-in binary and multi-class Classification Metrics.


    """
    if frame is None:
        raise ValueError("frame cannot be None")

    tc = frame._tc
    _scala_obj = get_scala_obj(tc)
    seed = int(os.urandom(2).encode('hex'), 16) if seed is None else seed
    scala_model = _scala_obj.train(frame._scala,
                                   tc.jutils.convert.to_scala_list_string(observation_columns),
                                   label_column,
                                   num_classes,
                                   num_trees,
                                   impurity,
                                   max_depth,
                                   max_bins,
                                   seed,
                                   __get_categorical_features_info(tc, categorical_features_info),
                                   tc.jutils.convert.to_scala_option(feature_subset_category),
                                   tc.jutils.convert.to_scala_option(min_instances_per_node),
                                   tc.jutils.convert.to_scala_option(sub_sampling_rate))

    return RandomForestClassifierModel(tc, scala_model)

def __get_categorical_features_info(tc, c):
    if c is not None:
        c = tc.jutils.convert.to_scala_map(c)
    return tc.jutils.convert.to_scala_option(c)


def load(path, tc=TkContext.implicit):
    """load RandomForestClassifierModel from given path"""
    TkContext.validate(tc)
    return tc.load(path, RandomForestClassifierModel)


def get_scala_obj(tc):
    """Gets reference to the scala object"""
    return tc.sc._jvm.org.trustedanalytics.sparktk.models.classification.random_forest_classifier.RandomForestClassifierModel


class RandomForestClassifierModel(PropertiesObject):
    """
    A trained Random Forest Classifier model

    Example
    -------

        >>> frame = tc.frame.create([[1,19.8446136104,2.2985856384],[1,16.8973559126,2.6933495054],
        ...                                 [1,5.5548729596,2.7777687995],[0,46.1810010826,3.1611961917],
        ...                                 [0,44.3117586448,3.3458963222],[0,34.6334526911,3.6429838715]],
        ...                                 [('Class', int), ('Dim_1', float), ('Dim_2', float)])

        >>> frame.inspect()
        [#]  Class  Dim_1          Dim_2
        =======================================
        [0]      1  19.8446136104  2.2985856384
        [1]      1  16.8973559126  2.6933495054
        [2]      1   5.5548729596  2.7777687995
        [3]      0  46.1810010826  3.1611961917
        [4]      0  44.3117586448  3.3458963222
        [5]      0  34.6334526911  3.6429838715

        >>> model = tc.models.classification.random_forest_classifier.train(frame, ['Dim_1', 'Dim_2'], 'Class', num_classes=2, num_trees=1, impurity="entropy", max_depth=4, max_bins=100)

        >>> model.feature_importances()
        {u'Dim_1': 1.0, u'Dim_2': 0.0}

        >>> predicted_frame = model.predict(frame, ['Dim_1', 'Dim_2'])

        >>> predicted_frame.inspect()
        [#]  Class  Dim_1          Dim_2         predicted_class
        ========================================================
        [0]      1  19.8446136104  2.2985856384                1
        [1]      1  16.8973559126  2.6933495054                1
        [2]      1   5.5548729596  2.7777687995                1
        [3]      0  46.1810010826  3.1611961917                0
        [4]      0  44.3117586448  3.3458963222                0
        [5]      0  34.6334526911  3.6429838715                0

        >>> test_metrics = model.test(frame, ['Dim_1','Dim_2'], 'Class')

        >>> test_metrics
        accuracy         = 1.0
        confusion_matrix =             Predicted_Pos  Predicted_Neg
        Actual_Pos              3              0
        Actual_Neg              0              3
        f_measure        = 1.0
        precision        = 1.0
        recall           = 1.0

        >>> model.save("sandbox/randomforestclassifier")

        >>> restored = tc.load("sandbox/randomforestclassifier")

        >>> restored.label_column == model.label_column
        True

        >>> restored.seed == model.seed
        True

        >>> set(restored.observation_columns) == set(model.observation_columns)
        True

    The trained model can also be exported to a .mar file, to be used with the scoring engine:

        >>> canonical_path = model.export_to_mar("sandbox/rfClassifier.mar")

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
        """Loads a random forest classifier model from a scala model"""
        return RandomForestClassifierModel(tc, scala_model)

    @property
    def label_column(self):
        """column containing the label used for model training"""
        return self._scala.labelColumn()

    @property
    def observation_columns(self):
        """observation columns used for model training"""
        return self._tc.jutils.convert.from_scala_seq(self._scala.observationColumns())

    @property
    def num_classes(self):
        """number of classes in the trained model"""
        return self._scala.numClasses()

    @property
    def num_trees(self):
        """number of trees in the trained model"""
        return self._scala.numTrees()

    @property
    def impurity(self):
        """impurity value of the trained model"""
        return self._scala.impurity()

    @property
    def max_depth(self):
        """maximum depth of the trained model"""
        return self._scala.maxDepth()

    @property
    def max_bins(self):
        """maximum bins in the trained model"""
        return self._scala.maxBins()

    @property
    def seed(self):
        """seed used during training of the model"""
        return self._scala.seed()

    @property
    def categorical_features_info(self):
        """categorical feature dictionary used during model training"""
        s = self._tc.jutils.convert.from_scala_option(self._scala.categoricalFeaturesInfo())
        if s:
            return self._tc.jutils.convert.scala_map_to_python(s)
        return None

    @property
    def feature_subset_category(self):
        """feature subset category of the trained model"""
        return self._tc.jutils.convert.from_scala_option(self._scala.featureSubsetCategory())

    @property
    def min_instances_per_node(self):
        """minimum number of instances per node of the trained model"""
        return self._tc.jutils.convert.from_scala_option(self._scala.minInstancesPerNode())

    @property
    def sub_sampling_rate(self):
        """sub sampling rate of the trained model"""
        return self._tc.jutils.convert.from_scala_option(self._scala.subSamplingRate())

    def feature_importances(self):
        """
        Feature importances for trained model. Higher values indicate more important features.
        :return: (Dict(str -> int)) A dictionary of feature names and importances
        """
        return self._tc.jutils.convert.scala_map_to_python(self._scala.featureImportances())

    def predict(self, frame, observation_columns=None):
        """
        Predict the labels for a test frame using trained Random Forest Classifier model, and create a new frame
        revision with existing columns and a new predicted label's column.

        Parameters
        ----------

        :param frame: (Frame) A frame whose labels are to be predicted. By default, predict is run on the same columns
                      over which the model is trained.
        :param observation_columns: (Optional(list[str])) Column(s) containing the observations whose labels are to be predicted.
                                    By default, the same observation column names from training are used
        :return: (Frame) A new frame consisting of the existing columns of the frame and a new column with predicted
                 label for each observation.
        """
        columns_list = affirm_type.list_of_str(observation_columns, "observation_columns", allow_none=True)
        columns_option = self._tc.jutils.convert.to_scala_option_list_string(columns_list)
        return Frame(self._tc, self._scala.predict(frame._scala, columns_option))

    def test(self, frame, observation_columns=None, label_column=None):
        """
        Predict test frame labels and return metrics.

        Parameters
        ----------

        :param frame: (Frame) The frame whose labels are to be predicted
        :param observation_columns: (Optional(list[str])) Column(s) containing the observations whose labels are to be predicted.
                                    By default, the same observation column names from training are used
        :param label_column: (str) Column containing the name of the label
                                    By default, the same label column name from training is used
        :return: (ClassificationMetricsValue) Binary classification metrics comprised of:
                accuracy (double)
                The proportion of predictions that are correctly identified
                confusion_matrix (dictionary)
                A table used to describe the performance of a classification model
                f_measure (double)
                The harmonic mean of precision and recall
                precision (double)
                The proportion of predicted positive instances that are correctly identified
                recall (double)
                The proportion of positive instances that are correctly identified.
        """
        columns_list = affirm_type.list_of_str(observation_columns, "observation_columns", allow_none=True)
        return ClassificationMetricsValue(self._tc, self._scala.test(frame._scala,
                                                                     self._tc.jutils.convert.to_scala_option_list_string(columns_list),
                                                                     self._tc.jutils.convert.to_scala_option(label_column)))

    def save(self, path):
        """
        Save the trained model to path

        Parameters
        ----------

        :param path: (str) Path to save
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

del PropertiesObject
