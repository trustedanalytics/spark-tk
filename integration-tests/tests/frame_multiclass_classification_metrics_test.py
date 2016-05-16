from setup import tc, rm, get_sandbox_path
from sparktk.dtypes import float32

# Tests multi-class classification with string values
def test_multiclass_classification_metrics_001(tc):
    print "create frame"
    rows = [["red", "red"],["blue", "green"],["green", "green"],["green", "green"],["orange","orange"],["red","orange"]]
    schema = [('labels', str),('predictions', str)]
    frame = tc.to_frame(rows, schema)

    assert(frame.row_count, 4, "frame should have 6 rows")
    assert(frame.column_names, ['labels', 'predictions'])

    print "compute multiclass_classification_metrics()"
    cm = frame.multiclass_classification_metrics('labels', 'predictions', 1)

    assert(cm.f_measure, 0.6, "computed f_measure for this model should be equal to 0.6")
    assert(cm.recall, 0.666666666667, "computed recall for this model should be equal to 0.666666666667")
    assert(cm.accuracy, 0.666666666667, "computed accuracy for this model should be equal to 0.666666666667")
    assert(cm.precision, 0.638888888889, "computed precision for this model should be equal to 0.638888888889")

    confusion_matrix = cm.confusion_matrix.values.tolist()
    assert(confusion_matrix, [[1,0,0],[2,0,0],[0,1,0],[0,1,1]], "computed confusion_matrix for this models should be equal to [1,0,0],[2,0,0],[0,1,0],[0,1,1]")

    # Tests multi-class classification with float values and missing values
def test_multiclass_classification_metrics_002(tc):
    print "create frame"
    rows = [[0.0, 0.0],[None, 0.0],[0.0, 0.0],[1.5, 1.5],[1.0, 1.0],[1.5, None]]
    schema = [('labels', float32),('predictions', float32)]
    frame = tc.to_frame(rows, schema)

    assert(frame.row_count, 4, "frame should have 6 rows")
    assert(frame.column_names, ['labels', 'predictions'])

    print "compute multiclass_classification_metrics()"
    cm = frame.multiclass_classification_metrics('labels', 'predictions', 1)

    assert(cm.f_measure, 0.627777777778, "computed f_measure for this model should be equal to 0.627777777778")
    assert(cm.recall, 0.666666666667, "computed recall for this model should be equal to 0.666666666667")
    assert(cm.accuracy, 0.666666666667, "computed accuracy for this model should be equal to 0.666666666667")
    assert(cm.precision, 0.805555555556, "computed precision for this model should be equal to 0.805555555556")

    confusion_matrix = cm.confusion_matrix.values.tolist()
    assert(confusion_matrix, [[2,0,0],[0,1,0],[1,1,1]], "computed confusion_matrix for this models should be equal to [2,0,0],[0,1,0],[1,1,1]")
