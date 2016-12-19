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


def grid_search(train_frame, test_frame, train_descriptors, tc= TkContext.implicit):
    """
    Implements grid search by training the specified model on all combinations of descriptor and testing on test frame
    :param train_frame: The frame to train the model on
    :param test_frame: The frame to test the model on
    :param model_type: The model reference
    :param descriptor: Dictionary of model parameters and their value/values in list of type grid_values
    :param tc: spark-tk context
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
        list_of_kwargs =  expand_kwarg_grids([descriptor.kwargs])
        for kwargs in list_of_kwargs:
            train_kwargs = dict(kwargs)
            train_kwargs['frame'] = train_frame
            validate_call(train_method, train_kwargs, ignore_self=True)
            model = descriptor.model_type.train(**train_kwargs)
            global count
            test_kwargs = dict(kwargs)
            test_kwargs['frame'] = test_frame
            test_kwargs = extract_call(model.test, test_kwargs, ignore_self=True)
            metrics = model.test(**test_kwargs)
            grid_points.append(GridPoint(descriptor=TrainDescriptor(descriptor.model_type, train_kwargs), metrics=metrics))
            count += 1  # sanity count
    return GridSearchResults(grid_points)


class MetricsCompare(object):

    def __init__(self, emphasis="accuracy", compare=None):
        self.compare = compare or self._get_compare(emphasis)

    def is_a_better_than_b(self, a, b):
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


class Metrics(ClassificationMetricsValue):

    def __init__(self):
        super(Metrics, self).__init__(None, None)

    def _divide(self, denominator):
        self.precision = (self.precision / float(denominator))
        self.accuracy = (self.accuracy / float(denominator))
        self.recall = (self.recall / float(denominator))
        self.f_measure = (self.f_measure / float(denominator))

    @staticmethod
    def _create_metric_sum(a, b):
        metric_sum = Metrics()
        metric_sum.accuracy = a.accuracy + b.accuracy
        metric_sum.precision = a.precision + b.precision
        metric_sum.f_measure = a.f_measure + b.f_measure
        metric_sum.recall = a.recall + b.recall
        metric_sum.confusion_matrix = a.confusion_matrix + b.confusion_matrix
        return metric_sum


GridValues = namedtuple('GridValues', ['args'])

class TrainDescriptor(object):

    def __init__(self, model_type, kwargs):
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


def grid_values(*args):
    return GridValues(args)

count = 0


def expand_kwarg_grids(dictionaries):
    """
    Method to expand the dictionary of arguments
    :param dictionaries: Parameters for the model
    :return: Expanded list of parameters for the model
    """
    if not isinstance(dictionaries, list):
        raise ValueError("descriptors was not a list but: %s" % dictionaries)
    new_dictionaries = []
    for dictionary in dictionaries:
        for k, v in dictionary.items():
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

    def __init__(self, grid_points, metrics_compare=None):
        self.grid_points = grid_points
        # add require_type for metrics_compare
        self.metrics_compare = metrics_compare or MetricsCompare()

    def copy(self):
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
        if len(self.grid_points) != len(points):
            raise ValueError("Expected list of points of len %s, got %s" % (len(self.grid_points), len(points)))

        for index in xrange(len(self.grid_points)):
            self._validate_descriptors_are_equal(self.grid_points[index].descriptor, points[index].descriptor, ["frame"])
            m = Metrics._create_metric_sum(self.grid_points[index].metrics, points[index].metrics)
            self.grid_points[index] = GridPoint(self.grid_points[index].descriptor, m)

    def _divide_metrics(self, denominator):
        for point in self.grid_points:
            point.metrics._divide(denominator)
