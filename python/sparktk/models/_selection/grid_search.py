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
from collections import namedtuple
from sparktk.arguments import extract_call, validate_call
from sparktk.frame.frame import Frame
from sparktk import arguments


def grid_values(*args):
    """
    Method that returns the args as a named tuple of GridValues and list of args
    :param args: Value/s passed for the model's  parameter
    :return: named tuple of GridValues and list of args
    """
    return GridValues(args)


def grid_search(train_frame, test_frame, train_descriptors, tc= TkContext.implicit):
    """
    Implements grid search by training the specified model on all combinations of descriptor and testing on test frame
    :param train_frame: The frame to train the model on
    :param test_frame: The frame to test the model on
    :param train_descriptors: Tuple of model and Dictionary of model parameters and their value/values as singleton
            values or a list of type grid_values
    :param tc: spark-tk context passed implicitly
    :return: Summary of metrics for different combinations of the grid and the best performing parameter combination

    Example
    -------

        >>> frame = tc.frame.create([[1,0],[2,0],[3,0],[4,0],[5,0],[6,1],[7,1],[8,1],[9,1],[10,1]],[("data", float),("label",int)])

        >>> frame.inspect()
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

        >>> grid_result = tc.models.grid_search(frame, frame,
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

        >>> grid_result
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

        >>> grid_result.find_best()
        GridPoint(descriptor=sparktk.models.classification.logistic_regression: {'num_iterations': 10, 'step_size': 0.01, 'observation_columns': 'data', 'label_column': 'label'}, metrics=accuracy         = 1.0
        confusion_matrix =             Predicted_Pos  Predicted_Neg
        Actual_Pos              5              0
        Actual_Neg              0              5
        f_measure        = 1.0
        precision        = 1.0
        recall           = 1.0)

        >>> grid_result.grid_points
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

        >>> grid_result.grid_points[1]
        GridPoint(descriptor=sparktk.models.classification.svm: {'num_iterations': 10, 'step_size': 0.01, 'observation_columns': 'data', 'label_column': 'label'}, metrics=accuracy         = 0.5
        confusion_matrix =             Predicted_Pos  Predicted_Neg
        Actual_Pos              5              0
        Actual_Neg              5              0
        f_measure        = 0.666666666667
        precision        = 0.5
        recall           = 1.0)

    """

    #validate input
    TkContext.validate(tc)
    if not isinstance(train_descriptors, list):
        train_descriptors = [train_descriptors]
    descriptors = [TrainDescriptor(x[0], x[1]) for x in train_descriptors if not isinstance(x, TrainDescriptor)]

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
    return GridSearchResults(grid_points)


class MetricsCompare(object):
    """
    Class to compare the classification metrics and pick the best performing model configuration based on 'accuracy'
    """

    def __init__(self, emphasis="accuracy", compare=None):
        """
        Initializes the object of class MetricsCompare
        :param emphasis: The metric to be compared on. We are initializing this to 'accuracy'
        :param compare: The objects to be compared
        """
        self.compare = compare or self._get_compare(emphasis)

    def is_a_better_than_b(self, a, b):
        """
        Method to compare two metrics objects
        :param a: First object
        :param b: Second object
        :return: Determines if a is better than b
        """
        result = self.compare(a, b)
        return result > 0

    @staticmethod
    def _get_compare(emphasis):
        def default_compare(a, b):
            a_value = getattr(a, emphasis)
            b_value = getattr(b, emphasis)
            if a_value > b_value:
                return 1
            if a_value < b_value:
                return -1
            return 0
        return default_compare


class GridSearchClassificationMetrics(ClassificationMetricsValue):
    """
    Class containing the results of grid_search for classification
    """

    def __init__(self):
        super(GridSearchClassificationMetrics, self).__init__(None, None)

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


GridValues = namedtuple('GridValues', ['args'])


class TrainDescriptor(object):
    """
    Class that separates the model type and args from the input and handles the representation.
    """

    def __init__(self, model_type, kwargs):
        """
        Initializes the model_type and model's arguments
        :param model_type: The name of the model
        :param kwargs: The list of model parameters
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

    def __init__(self, grid_points, metrics_compare=None):
        """
        Initializes the GridSearchResults class object with the grid_points and the metric to compare the results
        :param grid_points: The results of the grid_search computation
        :param metrics_compare: The user specified metric to compare the results on
        """
        self.grid_points = grid_points
        # add require_type for metrics_compare
        self.metrics_compare = metrics_compare or MetricsCompare()

    def copy(self):
        """
        Copies the GridSearchResults into the desired format
        :return: GridSearchResults object
        """
        return GridSearchResults([GridPoint(gp.descriptor, gp.metrics) for gp in self.grid_points], self.metrics_compare)

    def __repr__(self):
        return "\n".join([str(gp) for gp in self.grid_points])

    def find_best(self, metrics_compare=None):
        """
        Method to compare the list of all GridPoints and return the one with best accuracy
        :param metrics_compare: List of GridPoints to compare
        :return: The GridPoint with best accuracy
        """
        comparator = metrics_compare or self.metrics_compare
        if not self.grid_points:
            raise RuntimeError("GridSearchResults are empty, cannot find a best point")
        best = self.grid_points[0]
        for point in self.grid_points:
            if comparator.is_a_better_than_b(point.metrics, best.metrics):
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
            m = GridSearchClassificationMetrics._create_metric_sum(self.grid_points[index].metrics, points[index].metrics)
            self.grid_points[index] = GridPoint(self.grid_points[index].descriptor, m)

    def _divide_metrics(self, denominator):
        """
        Method to compute the average metrics
        :param denominator: The denominator for avergae computation
        :return: Averaged values of the metrics
        """
        for point in self.grid_points:
            point.metrics._divide(denominator)
