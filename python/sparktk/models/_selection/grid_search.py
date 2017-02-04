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


from sparktk import TkContext
from sparktk.frame.ops.classification_metrics_value import ClassificationMetricsValue
from sparktk.models.regression.regression_test_metrics import RegressionTestMetrics
from collections import namedtuple
from sparktk.arguments import extract_call, validate_call, require_type, affirm_type, value_error
from sparktk.frame.frame import Frame
from sparktk import arguments


def grid_values(*args):
    """
    Method that returns the args as a named tuple of GridValues and list of args
    :param args: Value/s passed for the model's  parameter
    :return: named tuple of GridValues and list of args
    """
    return GridValues(args)


def grid_search(train_frame, test_frame, train_descriptors, metrics_eval_func=None, tc= TkContext.implicit):
    """
    Implements grid search by training the specified classification or regression model on all combinations of descriptor and testing on test frame
    :param train_frame: The frame to train the model on
    :param test_frame: The frame to test the model on
    :param train_descriptors: Tuple of model and Dictionary of model parameters and their value/values as singleton
            values or a list of type grid_values
    :param metrics_eval_func: Custom function to compare metrics when determining best results.
            Default metric for classifiers is greater accuracy and for regressors is greater r2
    :param tc: spark-tk context passed implicitly
    :return: Summary of metrics for different combinations of the grid and the best performing parameter combination

    Example
    -------

        >>> classification_frame = tc.frame.create([[1,0],[2,0],[3,0],[4,0],[5,0],[6,1],[7,1],[8,1],[9,1],[10,1]],[("data", float),("label",int)])

        >>> classification_frame.inspect()
        [#]  data  label
        ================
        [0]     1      0
        [1]     2      0
        [2]     3      0
        [3]     4      0
        [4]     5      0
        [5]     6      1
        [6]     7      1
        [7]     8      1
        [8]     9      1
        [9]    10      1

        >>> from sparktk.models import grid_values

        >>> classification_grid = tc.models.grid_search(classification_frame, classification_frame,
        ...                                    [(tc.models.classification.svm,
        ...                                     {"observation_columns":"data",
        ...                                      "label_column":"label",
        ...                                      "num_iterations": grid_values(2, 10),
        ...                                      "step_size": 0.01}),
        ...                                     (tc.models.classification.logistic_regression,
        ...                                     {"observation_columns":"data",
        ...                                      "label_column":"label",
        ...                                      "num_iterations": grid_values(2, 10),
        ...                                      "step_size": 0.01})])

        >>> classification_grid
        GridPoint(descriptor=sparktk.models.classification.svm: {'num_iterations': 2, 'step_size': 0.01, 'observation_columns': 'data', 'label_column': 'label'}, metrics=accuracy         = 0.5
        confusion_matrix =             Predicted_Pos  Predicted_Neg
        Actual_Pos              5              0
        Actual_Neg              5              0
        f_measure        = 0.666666666667
        precision        = 0.5
        recall           = 1.0)
        GridPoint(descriptor=sparktk.models.classification.svm: {'num_iterations': 10, 'step_size': 0.01, 'observation_columns': 'data', 'label_column': 'label'}, metrics=accuracy         = 0.5
        confusion_matrix =             Predicted_Pos  Predicted_Neg
        Actual_Pos              5              0
        Actual_Neg              5              0
        f_measure        = 0.666666666667
        precision        = 0.5
        recall           = 1.0)
        GridPoint(descriptor=sparktk.models.classification.logistic_regression: {'num_iterations': 2, 'step_size': 0.01, 'observation_columns': 'data', 'label_column': 'label'}, metrics=accuracy         = 0.5
        confusion_matrix =             Predicted_Pos  Predicted_Neg
        Actual_Pos              5              0
        Actual_Neg              5              0
        f_measure        = 0.666666666667
        precision        = 0.5
        recall           = 1.0)
        GridPoint(descriptor=sparktk.models.classification.logistic_regression: {'num_iterations': 10, 'step_size': 0.01, 'observation_columns': 'data', 'label_column': 'label'}, metrics=accuracy         = 1.0
        confusion_matrix =             Predicted_Pos  Predicted_Neg
        Actual_Pos              5              0
        Actual_Neg              0              5
        f_measure        = 1.0
        precision        = 1.0
        recall           = 1.0)

        >>> classification_grid.find_best()
        GridPoint(descriptor=sparktk.models.classification.logistic_regression: {'num_iterations': 10, 'step_size': 0.01, 'observation_columns': 'data', 'label_column': 'label'}, metrics=accuracy         = 1.0
        confusion_matrix =             Predicted_Pos  Predicted_Neg
        Actual_Pos              5              0
        Actual_Neg              0              5
        f_measure        = 1.0
        precision        = 1.0
        recall           = 1.0)

        >>> classification_grid.grid_points
        [GridPoint(descriptor=sparktk.models.classification.svm: {'num_iterations': 2, 'step_size': 0.01, 'observation_columns': 'data', 'label_column': 'label'}, metrics=accuracy         = 0.5
        confusion_matrix =             Predicted_Pos  Predicted_Neg
        Actual_Pos              5              0
        Actual_Neg              5              0
        f_measure        = 0.666666666667
        precision        = 0.5
        recall           = 1.0),
         GridPoint(descriptor=sparktk.models.classification.svm: {'num_iterations': 10, 'step_size': 0.01, 'observation_columns': 'data', 'label_column': 'label'}, metrics=accuracy         = 0.5
        confusion_matrix =             Predicted_Pos  Predicted_Neg
        Actual_Pos              5              0
        Actual_Neg              5              0
        f_measure        = 0.666666666667
        precision        = 0.5
        recall           = 1.0),
         GridPoint(descriptor=sparktk.models.classification.logistic_regression: {'num_iterations': 2, 'step_size': 0.01, 'observation_columns': 'data', 'label_column': 'label'}, metrics=accuracy         = 0.5
        confusion_matrix =             Predicted_Pos  Predicted_Neg
        Actual_Pos              5              0
        Actual_Neg              5              0
        f_measure        = 0.666666666667
        precision        = 0.5
        recall           = 1.0),
         GridPoint(descriptor=sparktk.models.classification.logistic_regression: {'num_iterations': 10, 'step_size': 0.01, 'observation_columns': 'data', 'label_column': 'label'}, metrics=accuracy         = 1.0
        confusion_matrix =             Predicted_Pos  Predicted_Neg
        Actual_Pos              5              0
        Actual_Neg              0              5
        f_measure        = 1.0
        precision        = 1.0
        recall           = 1.0)]

        >>> classification_grid.grid_points[1]
        GridPoint(descriptor=sparktk.models.classification.svm: {'num_iterations': 10, 'step_size': 0.01, 'observation_columns': 'data', 'label_column': 'label'}, metrics=accuracy         = 0.5
        confusion_matrix =             Predicted_Pos  Predicted_Neg
        Actual_Pos              5              0
        Actual_Neg              5              0
        f_measure        = 0.666666666667
        precision        = 0.5
        recall           = 1.0)

        >>> regression_frame = tc.frame.create([[0, 0],[1,2.5],[2,5],[3,7.5],[4,10],[5,12.5],[6,13],[7,17.5],[8,18.5],[9,23.5]],[("x", float),("y",float)])

        >>> regression_frame.inspect()
        [#]  x  y
        ============
        [0]  0     0
        [1]  1   2.5
        [2]  2     5
        [3]  3   7.5
        [4]  4    10
        [5]  5  12.5
        [6]  6    13
        [7]  7  17.5
        [8]  8  18.5
        [9]  9  23.5

        >>> regression_grid = tc.models.grid_search(regression_frame, regression_frame,[(tc.models.regression.linear_regression,
        ...                                {"observation_columns":["x"], "label_column":"y",
        ...                                "max_iterations": grid_values(2, 10), "reg_param": 0.01}),
        ...                                (tc.models.regression.random_forest_regressor,
        ...                                {"observation_columns":"x", "label_column":"y",
        ...                                "num_trees": grid_values(1, 2), "max_depth": 4})],
        ...                                lambda a,b: getattr(a,"explained_variance")<getattr(b,"explained_variance"))

        <skip>
        >>> regression_grid
        GridPoint(descriptor=sparktk.models.regression.linear_regression: {'label_column': 'y', 'reg_param': 0.01, 'observation_columns': ['x'], 'max_iterations': 2}, metrics=explained_variance      = 49.5647448503
        mean_absolute_error     = 0.551091157145
        mean_squared_error      = 0.645552985861
        r2                      = 0.987178689457
        root_mean_squared_error = 0.803463120412)
        GridPoint(descriptor=sparktk.models.regression.linear_regression: {'label_column': 'y', 'reg_param': 0.01, 'observation_columns': ['x'], 'max_iterations': 10}, metrics=explained_variance      = 49.5647448503
        mean_absolute_error     = 0.551091157145
        mean_squared_error      = 0.645552985861
        r2                      = 0.987178689457
        root_mean_squared_error = 0.803463120412)
        GridPoint(descriptor=sparktk.models.regression.random_forest_regressor: {'label_column': 'y', 'max_depth': 4, 'observation_columns': 'x', 'num_trees': 1}, metrics=explained_variance      = 50.35
        mean_absolute_error     = 0.0
        mean_squared_error      = 0.0
        r2                      = 1.0
        root_mean_squared_error = 0.0)
        GridPoint(descriptor=sparktk.models.regression.random_forest_regressor: {'label_column': 'y', 'max_depth': 4, 'observation_columns': 'x', 'num_trees': 2}, metrics=explained_variance      = 23.24375
        mean_absolute_error     = 2.875
        mean_squared_error      = 18.24375
        r2                      = 0.637661370407
        root_mean_squared_error = 4.27127030285)

        >>> regression_grid.find_best()
        GridPoint(descriptor=sparktk.models.regression.random_forest_regressor: {'label_column': 'y', 'max_depth': 4, 'observation_columns': 'x', 'num_trees': 2}, metrics=explained_variance      = 23.24375
        mean_absolute_error     = 2.875
        mean_squared_error      = 18.24375
        r2                      = 0.637661370407
        root_mean_squared_error = 4.27127030285)
        </skip>

    """

    # validate input
    TkContext.validate(tc)
    descriptors = affirm_type.list_of_anything(train_descriptors, "train_descriptors")
    for i in xrange(len(descriptors)):
        item = descriptors[i]
        if not isinstance(item, TrainDescriptor):
            require_type(tuple, item, "item", "grid_search needs a list of items which are either of type TrainDescriptor or tuples of (model, train_kwargs)")
            if len(item) != 2:
                raise value_error("list requires tuples of len 2", item, "item in train_descriptors")
            if not hasattr(item[0], 'train'):
                raise value_error("first item in tuple needs to be a object with a 'train' function", item, "item in train_descriptors")
            descriptors[i] = TrainDescriptor(item[0], item[1])

    arguments.require_type(Frame, train_frame, "frame")
    arguments.require_type(Frame, test_frame, "frame")

    grid_points = []
    for descriptor in descriptors:
        train_method = getattr(descriptor.model_type, "train")
        list_of_kwargs = expand_kwarg_grids([descriptor.kwargs])
        for kwargs in list_of_kwargs:
            train_kwargs = dict(kwargs)
            train_kwargs['frame'] = train_frame
            validate_call(train_method, train_kwargs, ignore_self=True)
            model = descriptor.model_type.train(**train_kwargs)
            test_kwargs = dict(kwargs)
            test_kwargs['frame'] = test_frame
            test_kwargs = extract_call(model.test, test_kwargs, ignore_self=True)
            metrics = model.test(**test_kwargs)
            grid_points.append(GridPoint(descriptor=TrainDescriptor(descriptor.model_type, train_kwargs), metrics=metrics))
    return GridSearchResults(grid_points, metrics_eval_func)


class GridSearchClassificationMetrics(ClassificationMetricsValue):
    """
    Class containing the results of grid_search for classification
    """

    def __init__(self):
        super(GridSearchClassificationMetrics, self).__init__(None, None)

    @staticmethod
    def _default_eval_func(a, b):
        """
        Default evaluation function to pick model with higher accuracy
        :param a: ClassificationMetrics of first model
        :param b: ClassificationMetrics of second model
        :return: Model with higher accuracy
        """
        emphasis = "accuracy"
        a_value = getattr(a, emphasis)
        b_value = getattr(b, emphasis)
        return a_value > b_value

    def _divide(self, denominator):
        """
        Divides the classification metrics with a value for averaging
        :param denominator: The number used to compute the average
        :return: Average values for the classification metric
        """
        self.precision = (self.precision / float(denominator))
        self.accuracy = (self.accuracy / float(denominator))
        self.recall = (self.recall / float(denominator))
        self.f_measure = (self.f_measure / float(denominator))

    @staticmethod
    def _create_metric_sum(a, b):
        """
        Computes the sum of the classification metrics
        :param a: First element
        :param b: Second element
        :return: Sum of the ClassificationMetrics of a and b
        """
        metric_sum = GridSearchClassificationMetrics()
        metric_sum.accuracy = a.accuracy + b.accuracy
        metric_sum.precision = a.precision + b.precision
        metric_sum.f_measure = a.f_measure + b.f_measure
        metric_sum.recall = a.recall + b.recall
        metric_sum.confusion_matrix = a.confusion_matrix + b.confusion_matrix
        return metric_sum

    def get_metrics_class(self):
        """
        Method that gets the metrics class
        :return: ClassificationMetricsValue
        """
        return ClassificationMetricsValue


class GridSearchRegressionMetrics(RegressionTestMetrics):
    """
    Class containing the results of grid_search for regression
    """

    def __init__(self):
        super(GridSearchRegressionMetrics, self).__init__(None)

    @staticmethod
    def _default_eval_func(a, b):
        """
        Default evaluation function to pick model with higher r2
        :param a: RegressionTestMetrics of first model
        :param b: RegressionTestMetrics of second model
        :return: Model with higher r2
        """
        emphasis = "r2"
        a_value = getattr(a, emphasis)
        b_value = getattr(b, emphasis)
        return a_value > b_value

    def _divide(self, denominator):
        """
        Divides the regression metrics with a value for averaging
        :param denominator: The number used to compute the average
        :return: Average values for the regression metric
        """
        self.explained_variance = (self.explained_variance / float(denominator))
        self.mean_absolute_error = (self.mean_absolute_error / float(denominator))
        self.mean_squared_error = (self.mean_squared_error / float(denominator))
        self.r2 = (self.r2 / float(denominator))
        self.root_mean_squared_error = (self.root_mean_squared_error / float(denominator))

    @staticmethod
    def _create_metric_sum(a,b):
        """
        Computes the sum of the regression metrics
        :param a: First element
        :param b: Second element
        :return: Sum of the RegressionMetrics of a and b
        """
        metric_sum = GridSearchRegressionMetrics()
        metric_sum.explained_variance = a.explained_variance + b.explained_variance
        metric_sum.mean_absolute_error = a.mean_absolute_error + b.mean_absolute_error
        metric_sum.mean_squared_error = a.mean_squared_error + b.mean_squared_error
        metric_sum.r2 = a.r2 + b.r2
        metric_sum.root_mean_squared_error = a.root_mean_squared_error + b.root_mean_squared_error
        return metric_sum

    def get_metrics_class(self):
        """
        Method that gets the metrics class
        :return: ClassificationMetricsValue
        """
        return RegressionTestMetrics

GridValues = namedtuple('GridValues', ['args'])


class TrainDescriptor(object):
    """Describes a train operation: a model type and the arguments for its train method"""

    def __init__(self, model_type, kwargs):
        """
        Creates a TrainDescriptor
        :param model_type: type object representing the model in question
        :param kwargs: dict of key-value-pairs holding values for the train method's parameters
        """
        self.model_type = model_type
        self.kwargs = kwargs

    def __repr__(self):
        kw = dict(self.kwargs)
        del kw['frame']
        try:
            mt = self.model_type.__name__
        except:
            mt = str(self.model_type)
        return "%s: %s" % (mt, kw)


def expand_kwarg_grids(dictionaries):
    """
    Method to expand the dictionary of arguments
    :param dictionaries: Parameters for the model of type (list of dict)
    :return: Expanded list of parameters for the model
    """
    arguments.require_type(list, dictionaries, "dictionaries")
    new_dictionaries = []
    for dictionary in dictionaries:
        for k, v in dictionary.items():
            arguments.require_type(dict, dictionary, "item in dictionaries")
            if isinstance(v, GridValues):
                for a in v.args:
                    d = dictionary.copy()
                    d[k] = a
                    new_dictionaries.append(d)
                break
    if new_dictionaries:
        return expand_kwarg_grids(new_dictionaries)
    return dictionaries

GridPoint = namedtuple("GridPoint", ["descriptor", "metrics"])


class GridSearchResults(object):
    """
    Class that stores the results of grid_search. The classification metrics of all model configurations are stored.
    """

    def __init__(self, grid_points, metrics_eval_func=None):
        """
        Initializes the GridSearchResults class object with the grid_points and the metric to compare the results
        :param grid_points: The results of the grid_search computation
        :param metrics_eval_func: The user specified metric to compare the results on
        """
        self.grid_points = grid_points
        self.metrics_eval_func = metrics_eval_func or self._create_default_metrics_eval_func(grid_points)

    def _create_default_metrics_eval_func(self, grid_points):
        """
        Evaluate the GridPoints metrics and create the default evaluation function
        :param grid_points: List of GridPoints
        :return: Evaluation function depending on the type of the metrics object
        """
        eval_func = set([self._get_default_eval_func(point.metrics) for point in grid_points])
        if len(eval_func) != 1:
            raise RuntimeError("Error in retrieving evaluation function")
        return list(eval_func)[0]

    @staticmethod
    def _get_default_eval_func(metrics):
        """
        Based on the metrics object, return the default evaluation function
        :param metrics: Metrics object
        :return: The deafult evaluation function based on the type of metrics object
        """
        if isinstance(metrics, ClassificationMetricsValue):
            return GridSearchClassificationMetrics._default_eval_func
        elif isinstance(metrics, RegressionTestMetrics):
            return GridSearchRegressionMetrics._default_eval_func
        else:
            raise ValueError("Incorrect metrics object passed")

    def copy(self):
        """
        Copies the GridSearchResults into the desired format
        :return: GridSearchResults object
        """
        return GridSearchResults([GridPoint(gp.descriptor, gp.metrics) for gp in self.grid_points], self.metrics_eval_func)

    def __repr__(self):
        return "\n".join([str(gp) for gp in self.grid_points])

    def find_best(self, metrics_eval_func=None):
        """
        Method to compare the list of all GridPoints and return the one with best accuracy
        :param metrics_eval_func: List of GridPoints to compare
        :return: The GridPoint with best accuracy
        """
        eval_func = metrics_eval_func or self.metrics_eval_func
        if not self.grid_points:
            raise RuntimeError("GridSearchResults are empty, cannot find a best point")
        best = self.grid_points[0]
        for point in self.grid_points:
            if eval_func(point.metrics, best.metrics):
                best = point
        return best

    @staticmethod
    def _validate_descriptors_are_equal(a, b, ignore_args=None):
        """
        Method to compare if the two descriptors being compared are equal
        :param a: First descriptor
        :param b: Second descriptor
        :param ignore_args: List of descriptors that need to be ignored
        :return: Checks if the two descriptors being compared are equal and raises errors if they are not
        """
        if ignore_args is None:
            ignore_args = []
        if a.model_type != b.model_type:
            raise ValueError("Descriptors have different model types: %s vs %s" % (a.model_type, b.model_type))
        if len(a.kwargs) != len(b.kwargs):
            raise ValueError("Descriptors have kwargs of different length")
        for k, v in a.kwargs.items():
            if k not in b.kwargs:
                raise ValueError("Descriptors a != b because b is missing value for '%s'", k)
            if k not in ignore_args and b.kwargs[k] != v:
                raise ValueError("Descriptors a != b because of different values for '%s': %s != %s" % (k, v, b.kwargs[k]))

    def _accumulate_matching_points(self, points):
        """
        Method to compute the sum of the metrics for a given model and parameter configuration.
        :param points: Model and parameter configurations and the computed metrics
        :return: sum of the metrics for a given model and parameter configuration
        """
        if len(self.grid_points) != len(points):
            raise ValueError("Expected list of points of len %s, got %s" % (len(self.grid_points), len(points)))
        for index in xrange(len(self.grid_points)):
            self._validate_descriptors_are_equal(self.grid_points[index].descriptor, points[index].descriptor, ["frame"])
            if isinstance(points[index].metrics, ClassificationMetricsValue):
                m = GridSearchClassificationMetrics._create_metric_sum(self.grid_points[index].metrics, points[index].metrics)
            elif isinstance(points[index].metrics,  RegressionTestMetrics):
                m = GridSearchRegressionMetrics._create_metric_sum(self.grid_points[index].metrics, points[index].metrics)
            else:
                raise ValueError("Incorrect Metrics Class for '%s'", m)
            self.grid_points[index] = GridPoint(self.grid_points[index].descriptor, m)

    def _divide_metrics(self, denominator):
        """
        Method to compute the average metrics
        :param denominator: The denominator for avergae computation
        :return: Averaged values of the metrics
        """
        for point in self.grid_points:
            point.metrics._divide(denominator)
