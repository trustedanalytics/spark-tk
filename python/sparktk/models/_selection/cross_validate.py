# vim: set encoding=utf-8

#  Copyright (c) 2016 Intel Corporation
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

from sparktk import TkContext
from sparktk.frame.ops.classification_metrics_value import ClassificationMetricsValue

def cross_validate(frame, model_type, parameters, num_folds=3, metric=None, tc=TkContext.implicit):
    """
    Cross-validation

    :param frame:
    :param num_folds:
    :param model_type:
    :param parameters:
    :return:


    Examples
    --------

    <skip>
    >>> f = tc.frame.create([[3, 4], [0, 1]])
    >>> tc.models.soup(f, tc.models.classification.logistic_regression, {'observation_columns': 'C1', 'label_column': 'C0'})
    </skip>


    """
    from sparktk.frame.frame import Frame
    from sparktk import arguments

    TkContext.validate(tc)
    arguments.require_type(Frame, frame, "frame")

    train_method = getattr(model_type, "train")

    parameters["frame"] = frame
    arguments.validate_call(train_method, parameters)

    #Can I instantiate like this?
    overall_metric = ClassificationMetricsValue
    for validate_frame, train_frame in split_data(frame, num_folds, tc):
        parameters["frame"] = train_frame
        model = train_method(**parameters)
        parameters["frame"] = validate_frame
        test_model = model.test(validate_frame)
        update_metrics(test_model, overall_metric)

    return pretty_print_result(overall_metric, num_folds, metric)
        #return test_model
        #model.test(frame)
    #return cross_val_metrics


def split_data(frame, num_folds, tc=TkContext.implicit):
    from pyspark.sql.functions import rand
    df = frame.dataframe
    h = 1.0/num_folds
    rand_col = "rand_1"
    df = frame.select("*", rand(0).alias(rand_col))
    for i in xrange(num_folds):
        validation_lower_bound = i*h
        validation_upper_bound = (i+1)*h
        condition = (df[rand_col] >= validation_lower_bound) & (df[rand_col] < validation_upper_bound)
        validation_df = df.filter(condition)
        train_df = df.filter(~condition)
        train_frame = tc.frame.create(train_df)
        validation_frame = tc.frame.create(validation_df)
        yield validation_frame, train_frame


def update_metrics(current_fold_metric=ClassificationMetricsValue, overall_metric=ClassificationMetricsValue):
    overall_metric.accuracy += current_fold_metric.accuracy
    overall_metric.precision += current_fold_metric.precision
    overall_metric.recall += current_fold_metric.recall
    overall_metric.f_measure += current_fold_metric.f_measure
    overall_metric.confusion_matrix += current_fold_metric.confusion_matrix
    return overall_metric


def pretty_print_result(overall_metric, num_folds, metric):
    overall_metric.accuracy = overall_metric.accuracy/num_folds
    overall_metric.precision = overall_metric.precision/num_folds
    overall_metric.recall = overall_metric.recall/num_folds
    overall_metric.f_measure = overall_metric.f_measure/num_folds
    return overall_metric