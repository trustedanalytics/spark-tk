import pandas as pd
from sparktk.propobj import PropertiesObject

class ClassificationMetricsValue(PropertiesObject):
    """
    ClassificationMetricsValue class used to hold the data returned from binary_classification_metrics()
    and multiclass_classification_metrics().
    """
    def __init__(self, tc,  scala_result):
        self._tc = tc
        self._accuracy = scala_result.accuracy()
        cm = scala_result.confusionMatrix()
        if cm:
            self._confusion_matrix = cm
            column_list = self._tc.jutils.convert.from_scala_seq(cm.columnLabels())
            row_label_list = self._tc.jutils.convert.from_scala_seq(cm.rowLabels())
            header = ["Predicted_" + column.title() for column in column_list]
            row_index = ["Actual_" + row_label.title() for row_label in row_label_list]
            data = [list(x) for x in list(cm.getMatrix())]
            self._confusion_matrix = pd.DataFrame(data, index=row_index, columns=header)
        else:
            #empty pandas frame
            self._confusion_matrix = pd.DataFrame()

        self._f_measure = scala_result.fMeasure()
        self._precision = scala_result.precision()
        self._recall = scala_result.recall()

    @property
    def accuracy(self):
        return self._accuracy

    @property
    def confusion_matrix(self):
        return self._confusion_matrix

    @property
    def f_measure(self):
        return self._f_measure

    @property
    def precision(self):
        return self._precision

    @property
    def recall(self):
        return self._recall