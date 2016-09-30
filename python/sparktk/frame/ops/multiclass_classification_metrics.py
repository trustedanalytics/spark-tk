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

from classification_metrics_value import ClassificationMetricsValue

def multiclass_classification_metrics(self, label_column, pred_column, beta=1.0, frequency_column=None):
    """
    Statistics of accuracy, precision, and others for a multi-class classification model.

    Parameters:

    :param label_column: (str) The name of the column containing the correct label for each instance.
    :param pred_column: (str) The name of the column containing the predicted label for each instance.
    :param beta: (Optional[int]) This is the beta value to use for :math:`F_{ \beta}` measure (default F1 measure is computed);
                must be greater than zero. Defaults is 1.
    :param frequency_column: (Optional[str]) The name of an optional column containing the frequency of observations.
    :return: (ClassificationMetricsValue) The data returned is composed of multiple components:<br>
            &lt;object&gt;.accuracy : double<br>
            &lt;object&gt;.confusion_matrix : table<br>
            &lt;object&gt;.f_measure : double<br>
            &lt;object&gt;.precision : double<br>
            &lt;object&gt;.recall : double

   Calculate the accuracy, precision, confusion_matrix, recall and :math:`F_{ \beta}` measure for a
   classification model.

   *   The **f_measure** result is the :math:`F_{ \beta}` measure for a
       classification model.
       The :math:`F_{ \beta}` measure of a binary classification model is the
       harmonic mean of precision and recall.
       If we let:

       * beta :math:`\equiv \beta`,
       * :math:`T_{P}` denotes the number of true positives,
       * :math:`F_{P}` denotes the number of false positives, and
       * :math:`F_{N}` denotes the number of false negatives

       then:

       .. math::

           F_{ \beta} = (1 + \beta ^ 2) * \frac{ \frac{T_{P}}{T_{P} + F_{P}} * \
           \frac{T_{P}}{T_{P} + F_{N}}}{ \beta ^ 2 * \frac{T_{P}}{T_{P} + \
           F_{P}}  + \frac{T_{P}}{T_{P} + F_{N}}}

       The :math:`F_{ \beta}` measure for a multi-class classification model is
       computed as the weighted average of the :math:`F_{ \beta}` measure for
       each label, where the weight is the number of instances of each label.
       The determination of binary vs. multi-class is automatically inferred
       from the data.

   *   For multi-class classification models, the **recall** measure is computed as
       the weighted average of the recall for each label, where the weight is
       the number of instances of each label.
       The determination of binary vs. multi-class is automatically inferred
       from the data.

   *   For multi-class classification models, the **precision** measure is computed
       as the weighted average of the precision for each label, where the
       weight is the number of instances of each label.
       The determination of binary vs. multi-class is automatically inferred
       from the data.

   *   The **accuracy** of a classification model is the proportion of
       predictions that are correctly identified.
       If we let :math:`T_{P}` denote the number of true positives,
       :math:`T_{N}` denote the number of true negatives, and :math:`K` denote
       the total number of classified instances, then the model accuracy is
       given by: :math:`\frac{T_{P} + T_{N}}{K}`.

   *   The **confusion_matrix** result is a confusion matrix for a
       classifier model, formatted for human readability.

   Examples
   --------
   Consider Frame *my_frame*, which contains the data

       <hide>
       >>> s = [('a', str),('b', int),('labels', int),('predictions', int)]
       >>> rows = [["red", 1, 0, 0], ["blue", 3, 1, 0],["green", 1, 0, 0],["green", 0, 1, 1],["red", 0, 5, 4]]
       >>> my_frame = tc.frame.create(rows, s)
       -etc-

       </hide>
       >>> my_frame.inspect()
        [#]  a      b  labels  predictions
        ==================================
        [0]  red    1       0            0
        [1]  blue   3       1            0
        [2]  green  1       0            0
        [3]  green  0       1            1
        [4]  red    0       5            4

       >>> cm = my_frame.multiclass_classification_metrics('labels', 'predictions')
       <progress>

       >>> cm.f_measure
       0.5866666666666667

       >>> cm.recall
       0.6

       >>> cm.accuracy
       0.6

       >>> cm.precision
       0.6666666666666666

       >>> cm.confusion_matrix
                         Predicted_0  Predicted_1  Predicted_4
        Actual_0            2            0            0
        Actual_1            1            1            0
        Actual_5            0            0            1

    """
    return ClassificationMetricsValue(self._tc, self._scala.multiClassClassificationMetrics(label_column,
                                      pred_column,
                                      float(beta),
                                      self._tc.jutils.convert.to_scala_option(frequency_column)))