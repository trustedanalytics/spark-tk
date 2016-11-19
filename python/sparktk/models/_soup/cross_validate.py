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
#from sparktk.frame.ops.classification_metrics_value import ClassificationMetricsValue

def cross_validate(frame, model_type, parameters, num_folds=4, metric=None, tc=TkContext.implicit):
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
    result = train_method(**parameters)
    return result

