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
from sparktk.frame.frame import Frame
from sparktk.arguments import affirm_type, require_type
from sparktk import TkContext
import os

__all__ = ["train", "load", "RandomForestRegressorModel"]

def train(frame,
          observation_columns,
          label_column,
          num_trees = 1,
          impurity = "variance",
          max_depth = 4,
          max_bins = 100,
          min_instances_per_node = 1,
          sub_sampling_rate = 1.0,
          feature_subset_category = "auto",
          seed = None,
          categorical_features_info = None):
    """
    Creates a Random Forest Regressor Model by training on the given frame

    Parameters
    ----------

    :param frame: (Frame) frame frame of training data
    :param observation_columns: (list(str)) Column(s) containing the observations
    :param label_column: (str) Column name containing the label for each observation
    :param num_trees: (int) Number of tress in the random forest. Default is 1
    :param impurity: (str) Criterion used for information gain calculation. Default value is "variance".
    :param max_depth: (int) Maximum depth of the tree. Default is 4
    :param max_bins: (int) Maximum number of bins used for splitting features.
    :param min_instances_per_node: (int) Minimum number of records each child node must have after a split.
    :param sub_sampling_rate: (double) Fraction between 0..1 of the training data used for learning each decision tree.
    :param feature_subset_category: (str) Subset of observation columns, i.e., features,
                                 to consider when looking for the best split.
                                 Supported values "auto","all","sqrt","log2","onethird".
                                 If "auto" is set, this is based on num_trees: if num_trees == 1, set to "all"
                                 ; if num_trees > 1, set to "sqrt".
    :param seed: (Optional(int)) Random seed for bootstrapping and choosing feature subsets. Default is a randomly chosen seed.
    :param categorical_features_info: (Optional(Dict(str:int))) Arity of categorical features. Entry (name-> k) indicates
                                      that feature 'name' is categorical with 'k' categories indexed from 0:{0,1,...,k-1}

    :return: (RandomForestRegressorModel) The trained random forest regressor model

    Notes
    -----
    Random Forest is a supervised ensemble learning algorithm used to perform regression. A Random Forest
    Regressor model is initialized, trained on columns of a frame, and used to predict the value of each
    observation in the frame. This model runs the Spark ML implementation of Random Forest. During training,
    the decision trees are trained in parallel. During prediction, the average over-all tree's predicted
    value is the predicted value of the random forest.

    """
    require_type(Frame, frame, 'frame')
    column_list = affirm_type.list_of_str(observation_columns, "observation_columns")
    require_type.non_empty_str(label_column, "label_column")
    require_type.non_negative_int(num_trees, "num_trees")
    require_type.non_empty_str(impurity, "impurity")
    require_type.non_negative_int(max_depth, "max_depth")
    require_type.non_negative_int(max_bins, "max_bins")
    require_type.non_negative_int(min_instances_per_node, "min_instances_per_node")
    require_type(float, sub_sampling_rate, "sub_sampling_rate")
    if sub_sampling_rate > 1 or sub_sampling_rate < 0:
        raise ValueError("'sub_sampling_rate' parameter must have a value between 0 and 1")
    require_type.non_empty_str(feature_subset_category, "feature_subset_category")

    tc = frame._tc
    _scala_obj = get_scala_obj(tc)
    seed = int(os.urandom(2).encode('hex'), 16) if seed is None else seed
    scala_model = _scala_obj.train(frame._scala,
                                   tc.jutils.convert.to_scala_list_string(column_list),
                                   label_column,
                                   num_trees,
                                   impurity,
                                   max_depth,
                                   max_bins,
                                   min_instances_per_node,
                                   sub_sampling_rate,
                                   feature_subset_category,
                                   seed,
                                   __get_categorical_features_info(tc, categorical_features_info))

    return RandomForestRegressorModel(tc, scala_model)


def __get_categorical_features_info(tc, c):
    if c is not None:
        c = tc.jutils.convert.to_scala_map(c)
    return tc.jutils.convert.to_scala_option(c)


def load(path, tc=TkContext.implicit):
    """load RandomForestRegressorModel from given path"""
    TkContext.validate(tc)
    return tc.load(path, RandomForestRegressorModel)


def get_scala_obj(tc):
    """Gets reference to the scala object"""
    return tc.sc._jvm.org.trustedanalytics.sparktk.models.regression.random_forest_regressor.RandomForestRegressorModel


class RandomForestRegressorModel(PropertiesObject):
    """
    A trained Random Forest Regressor model

    Example
    -------

        >>> frame = tc.frame.create([[1,19.8446136104,2.2985856384],[1,16.8973559126,2.6933495054],
        ...                          [1,5.5548729596,2.7777687995],[0,46.1810010826,3.1611961917],
        ...                          [0,44.3117586448,3.3458963222],[0,34.6334526911,3.6429838715]],
        ...                          [('Class', int), ('Dim_1', float), ('Dim_2', float)])

        >>> frame.inspect()
        [#]  Class  Dim_1          Dim_2
        =======================================
        [0]      1  19.8446136104  2.2985856384
        [1]      1  16.8973559126  2.6933495054
        [2]      1   5.5548729596  2.7777687995
        [3]      0  46.1810010826  3.1611961917
        [4]      0  44.3117586448  3.3458963222
        [5]      0  34.6334526911  3.6429838715

        >>> model = tc.models.regression.random_forest_regressor.train(frame,
        ...                                                                ['Dim_1', 'Dim_2'],
        ...                                                                'Class',
        ...                                                                num_trees=1,
        ...                                                                impurity="variance",
        ...                                                                max_depth=4,
        ...                                                                max_bins=100)

        >>> model.feature_importances()
        {u'Dim_1': 1.0, u'Dim_2': 0.0}

        >>> predict_frame = model.predict(frame, ['Dim_1', 'Dim_2'])

        >>> predict_frame.inspect()
        [#]  Class  Dim_1          Dim_2         predicted_value
        ========================================================
        [0]      1  19.8446136104  2.2985856384                1.0
        [1]      1  16.8973559126  2.6933495054                1.0
        [2]      1   5.5548729596  2.7777687995                1.0
        [3]      0  46.1810010826  3.1611961917                0.0
        [4]      0  44.3117586448  3.3458963222                0.0
        [5]      0  34.6334526911  3.6429838715                0.0

        >>> random_forest_test_return = model.test(frame, label_column='Class')
        <progress>

        >>> random_forest_test_return
        explained_variance       = 0.25
        mean_absolute_error      = 0.0
        mean_squared_error       = 0.0
        r2                       = 1.0
        root_mean_squared_error  = 0.0

        >>> model.save("sandbox/randomforestregressor")

        >>> restored = tc.load("sandbox/randomforestregressor")

        >>> restored.label_column == model.label_column
        True

        >>> restored.seed == model.seed
        True

        >>> set(restored.observation_columns) == set(model.observation_columns)
        True

    The trained model can also be exported to a .mar file, to be used with the scoring engine:

        >>> canonical_path = model.export_to_mar("sandbox/rfRegressor.mar")

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
        """Loads a random forest regressor model from a scala model"""
        return RandomForestRegressorModel(tc, scala_model)

    @property
    def label_column(self):
        """column containing the labels used for model training"""
        return self._scala.labelColumn()

    @property
    def observation_columns(self):
        """observation columns used for model training"""
        return self._tc.jutils.convert.from_scala_seq(self._scala.observationColumns())

    @property
    def num_trees(self):
        """number of trees in the trained model"""
        return self._scala.numTrees()

    @property
    def impurity(self):
        """criterion used for calculating information gain in the trained model"""
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
        """dictionary of categorical feature names and number of features per category used during model training"""
        s = self._tc.jutils.convert.from_scala_option(self._scala.categoricalFeaturesInfo())
        if s:
            return self._tc.jutils.convert.scala_map_to_python(s)
        return None

    @property
    def feature_subset_category(self):
        """subset of observation columns, i.e., features, considered when splitting nodes in the trained model"""
        return self._scala.featureSubsetCategory()

    @property
    def min_instances_per_node(self):
        """minimum number of records in each child node of the trained model"""
        return self._scala.minInstancesPerNode()

    @property
    def sub_sampling_rate(self):
        """sub sampling rate of the trained model"""
        return self._scala.subSamplingRate()

    def feature_importances(self):
        """
        Feature importances for trained model. Higher values indicate more important features.

        Each feature's importance is the average of its importance across all trees in the ensemble.
        The importance vector is normalized to sum to 1.

        :return: (Dict(str -> int)) A dictionary of feature names and importances
        """
        return self._tc.jutils.convert.scala_map_to_python(self._scala.featureImportances())

    def predict(self, frame, observation_columns=None):
        """
        Predict the values for the data points.

        Predict the values for a test frame using trained Random Forest Classifier model, and create a new frame revision
        with existing columns and a new predicted value's column.

        Parameters
        ----------

        :param frame: (Frame) A frame whose labels are to be predicted. By default, predict is run on the same columns
                      over which the model is trained.
        :param observation_columns: (Optional(list[str])) Column(s) containing the observations whose labels are to be predicted.
                        By default, we predict the labels over columns the Random Forest model was trained on.
        :return: (Frame) A new frame consisting of the existing columns of the frame and a new column with predicted
                 value for each observation.
        """

        require_type(Frame, frame, 'frame')
        column_list = affirm_type.list_of_str(observation_columns, "observation_columns", allow_none=True)
        columns_option = self._tc.jutils.convert.to_scala_option_list_string(column_list)
        return Frame(self._tc, self._scala.predict(frame._scala, columns_option))

    def test(self, frame, observation_columns=None, label_column=None):
        """
        Test the frame given the trained model

        Parameters
        ----------

        :param frame: (Frame) The frame to predict on
        :param observation_columns: Optional(List[str]) List of column(s) containing the observations
        :param label_column: Optional(String) Column name containing the label for each observation
        :return: (RegressionTestMetrics) RegressionTestMetrics object consisting of results from model test
        """
        require_type(Frame, frame, 'frame')
        column_list = affirm_type.list_of_str(observation_columns, "observation_columns", allow_none=True)
        obs = self._tc.jutils.convert.to_scala_option_list_string(column_list)
        label = self._tc.jutils.convert.to_scala_option(label_column)
        return RegressionTestMetrics(self._scala.test(frame._scala, obs, label))

    def __columns_to_option(self, c):
        if c is not None:
            c = self._tc.jutils.convert.to_scala_list_string(c)
        return self._tc.jutils.convert.to_scala_option(c)

    def save(self, path):
        """
        Save the trained model to path

        Parameters
        ----------

        :param path: (str) Path to save
        """
        require_type.non_empty_str(path, "path")
        self._scala.save(self._tc._scala_sc, path, False)

    def export_to_mar(self, path):
        """
        Exports the trained model as a model archive (.mar) to the specified path.

        Parameters
        ----------

        :param path: (str) Path to save the trained model
        :return: (str) Full path to the saved .mar file

        """
        require_type.non_empty_str(path, "path")
        return self._scala.exportToMar(self._tc._scala_sc, path)

del PropertiesObject
