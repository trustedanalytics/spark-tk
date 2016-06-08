from setup import tc, rm, get_sandbox_path


def test_naive_bayes(tc):

    print "define schema"
    schema = [("Class", int),("Dim_1", int),("Dim_2", int),("Dim_3", int)]

    print "creating the frame"
    data = [[0,1,0,0],
            [2,0,0,0],
            [1,0,1,0],
            [1,0,2,0],
            [2,0,0,1],
            [2,0,0,2]]
    f = tc.frame.create(data, schema=schema)
    print f.inspect()

    print "training the model on the frame"
    model = tc.models.classification.naive_bayes.train(f, 'Class', ['Dim_1', 'Dim_2', 'Dim_3'])
    print "predicting the class using the model and the frame"
    model.predict(f)
    assert(set(f.column_names) == set(['Class', 'Dim_1', 'Dim_2', 'Dim_3','predicted_class']))
    assert(len(f.column_names) == 5)
    metrics = model.test(f)
    assert(metrics.accuracy == 1.0)
    assert(metrics.f_measure == 1.0)
    assert(metrics.precision == 1.0)
    assert(metrics.recall == 1.0)


