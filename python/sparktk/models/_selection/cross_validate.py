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
from sparktk.propobj import PropertiesObject

def cross_validate(frame, model_type, parameters, num_folds=3, verbose=False, tc=TkContext.implicit):
    """
    Cross-validation

    :param frame:
    :param num_folds:
    :param model_type:
    :param parameters:
    :param num_folds:
    :param verbosity:
    :param tc:
    :return:


    Examples
    --------

    <skip>
    >>> f = tc.frame.create([[3, 4], [0, 1]])
    >>> tc.models.soup(f, tc.models.classification.logistic_regression, {'observation_columns': 'C1', 'label_column': 'C0'}, num_folds=4)
    </skip>


    """
    from sparktk.frame.frame import Frame
    from sparktk import arguments

    TkContext.validate(tc)
    arguments.require_type(Frame, frame, "frame")

    train_method = getattr(model_type, "train")

    parameters["frame"] = frame
    arguments.validate_call(train_method, parameters)

    fold_result = []
    for validate_frame, train_frame in split_data(frame, num_folds, tc):
        parameters["frame"] = train_frame
        model = train_method(**parameters)
        test_parameters = dict(parameters)
        test_parameters["frame"] = validate_frame
        call_test_parameters = arguments.find_arguments(model.test, test_parameters)
        test_model = model.test(**call_test_parameters)
        fold_result.append(test_model)
        #update_metrics(test_model, overall_metric)
    return ClassificationCrossValidateValue(fold_result, verbose)


def split_data(frame, num_folds, tc=TkContext.implicit):
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


class ClassificationCrossValidateValue(object):
    def __init__(self, classification_metrics=[], verbose=False):
        self.cv_list = classification_metrics
        self.verbose = verbose
        self.average = self._compute_overall_metrics(classification_metrics)

    # def _get_all_str(self):
    #     return "\n".join([repr(cm) for cm in self.cv_list])
    def _get_all_str(self):
        return "\n".join(["""Fold: %s\n""" % str(self.cv_list.index(cm)+1) + repr(cm) for cm in self.cv_list])


    def show_all(self):
        return self._get_all_str()

    def __repr__(self):
        result = self._get_all_str() if self.verbose else ''
        return result + """
Averages:
%s""" % self.average

    def _compute_overall_metrics(self, classification_metrics):
        overall_metric = ClassificationMetricsValue(classification_metrics[0].get_context, None)
        for i in classification_metrics:
            overall_metric.accuracy += i.accuracy
            overall_metric.precision += i.precision
            overall_metric.recall += i.recall
            overall_metric.f_measure += i.f_measure
            overall_metric.confusion_matrix += i.confusion_matrix
        overall_metric.accuracy /= len(classification_metrics)
        overall_metric.precision /= len(classification_metrics)
        overall_metric.recall /= len(classification_metrics)
        overall_metric.f_measure /= len(classification_metrics)
        return overall_metric
