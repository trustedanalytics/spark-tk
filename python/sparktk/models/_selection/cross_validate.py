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
from grid_search import grid_values, expand_descriptors, grid_search, GridPoint, GridSearchResults


def cross_validate(frame, model_type, descriptor, num_folds=3, verbose=False, tc=TkContext.implicit):
    """

    :param frame:
    :param model_type:
    :param descriptor:
    :param num_folds:
    :param verbose:
    :param tc:
    :return:
    """
    TkContext.validate(tc)
    arguments.require_type(Frame, frame, "frame")

    all_grid_search_results = []
    grid_search_results_accumulator = None
    for validate_frame, train_frame in split_data(frame, num_folds , tc):
        scores = grid_search(train_frame, validate_frame, model_type, descriptor , tc)
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

    :param frame:
    :param num_folds:
    :param tc:
    :return:
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
Averages:
%s""" % self.averages

