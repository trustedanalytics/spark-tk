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
from collections import namedtuple
from sparktk.frame.frame import Frame
from sparktk import arguments
from grid_search import grid_values, expand_kwarg_grids, grid_search, GridPoint, GridSearchResults


def cross_validate(frame, train_descriptors, num_folds=3, verbose=False, tc=TkContext.implicit):
    """
    Computes k-fold cross validation on model with the given frame and parameter values
    :param frame: The frame to perform cross-validation on
    :param model_type: The model reference
    :param descriptor: Dictionary of model parameters and their value/values in list of type grid_values
    :param num_folds: Number of folds to run the cross-validator on
    :param verbose: Flag indicating if the results of each fold are to be viewed. Default is set to False
    :param tc: spark-tk context
    :return: Summary of model's performance

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

        >>> result = tc.models.cross_validate(frame,
        ...                                   [(tc.models.classification.svm,
        ...                                     {"observation_columns":"data",
        ...                                      "label_column":"label",
        ...                                      "num_iterations": grid_values(2, 10),
        ...                                      "step_size": 0.01}),
        ...                                    (tc.models.classification.logistic_regression,
        ...                                     {"observation_columns":"data",
        ...                                      "label_column":"label",
        ...                                      "num_iterations": grid_values(2, 10),
        ...                                      "step_size": 0.01})],
        ...                                   num_folds=2,
        ...                                   verbose=True)

        <skip>
        >>> result
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
        GridPoint(descriptor=sparktk.models.classification.svm: {'num_iterations': 2, 'step_size': 0.01, 'observation_columns': 'data', 'label_column': 'label'}, metrics=accuracy         = 0.5
        confusion_matrix =             Predicted_Pos  Predicted_Neg
        Actual_Pos              2              0
        Actual_Neg              2              0
        f_measure        = 0.666666666667
        precision        = 0.5
        recall           = 1.0)
        GridPoint(descriptor=sparktk.models.classification.svm: {'num_iterations': 10, 'step_size': 0.01, 'observation_columns': 'data', 'label_column': 'label'}, metrics=accuracy         = 0.5
        confusion_matrix =             Predicted_Pos  Predicted_Neg
        Actual_Pos              2              0
        Actual_Neg              2              0
        f_measure        = 0.666666666667
        precision        = 0.5
        recall           = 1.0)
        GridPoint(descriptor=sparktk.models.classification.logistic_regression: {'num_iterations': 2, 'step_size': 0.01, 'observation_columns': 'data', 'label_column': 'label'}, metrics=accuracy         = 0.5
        confusion_matrix =             Predicted_Pos  Predicted_Neg
        Actual_Pos              2              0
        Actual_Neg              2              0
        f_measure        = 0.666666666667
        precision        = 0.5
        recall           = 1.0)
        GridPoint(descriptor=sparktk.models.classification.logistic_regression: {'num_iterations': 10, 'step_size': 0.01, 'observation_columns': 'data', 'label_column': 'label'}, metrics=accuracy         = 1.0
        confusion_matrix =             Predicted_Pos  Predicted_Neg
        Actual_Pos              2              0
        Actual_Neg              0              2
        f_measure        = 1.0
        precision        = 1.0
        recall           = 1.0)
        ******Averages: ******
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

        >>> result.averages
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
        </skip>
    """
    TkContext.validate(tc)
    arguments.require_type(Frame, frame, "frame")

    all_grid_search_results = []
    grid_search_results_accumulator = None
    for validate_frame, train_frame in split_data(frame, num_folds , tc):
        scores = grid_search(train_frame, validate_frame, train_descriptors, tc)
        if grid_search_results_accumulator is None:
            grid_search_results_accumulator = scores
        else:
            grid_search_results_accumulator._accumulate_matching_points(scores.grid_points)
        all_grid_search_results.append(scores)

    # make the accumulator hold averages
    grid_search_results_accumulator._divide_metrics(num_folds)
    return CrossValidateClassificationResults(all_grid_search_results,
                                              grid_search_results_accumulator.copy(),
                                              verbose)


def split_data(frame, num_folds, tc=TkContext.implicit):
    """
    Randomly split data based on num_folds specified. Implementation logic borrowed from pyspark.
    :param frame: The frame to be split into train and validation frames
    :param num_folds: Number of folds to be split into
    :param tc: spark-tk context
    :return: validation frame and train frame for each fold
    """
    from pyspark.sql.functions import rand
    df = frame.dataframe
    h = 1.0/num_folds
    rand_col = "rand_1"
    df_indexed = df.select("*", rand(0).alias(rand_col))
    for i in xrange(num_folds):
        validation_lower_bound = i*h
        validation_upper_bound = (i+1)*h
        condition = (df_indexed[rand_col] >= validation_lower_bound) & (df_indexed[rand_col] < validation_upper_bound)
        validation_df = df_indexed.filter(condition)
        train_df = df_indexed.filter(~condition)
        train_frame = tc.frame.create(train_df)
        validation_frame = tc.frame.create(validation_df)
        yield validation_frame, train_frame


class CrossValidateClassificationResults(object):
    def __init__(self, all_grid_search_results, averages, verbose=False):
        self.all_results = all_grid_search_results
        self.averages = averages
        self.verbose = verbose

    def _get_all_str(self):
        return "\n".join(["\n".join([str(point) for point in cm.grid_points]) for cm in self.all_results])

    def show_all(self):
        return self._get_all_str()

    def __repr__(self):
        result = self._get_all_str() if self.verbose else ''
        return result + """
******Averages: ******
%s""" % self.averages

