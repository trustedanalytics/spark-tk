from setup import tc


def _make_frame(tc):
    schema = [('name',str), ('age', int), ('tenure', int), ('phone', str)]
    rows = [['Fred', 39, 16, '555-1234'], ['Susan', 33, 3, '555-0202'], ['Thurston', 65, 26, '555-4510'], ['Judy', 44, 14, '555-2183']]
    frame = tc.frame.create(rows, schema)
    return frame


def test_take_python_backend(tc):
    frame = _make_frame(tc)
    data1 = frame.take(2, columns=['name', 'phone'])
    assert(data1 == [['Fred', '555-1234'], ['Susan', '555-0202']])

    data2 = frame.take(2, offset=2)
    assert(data2 == [['Thurston', 65, 26, '555-4510'], ['Judy', 44, 14, '555-2183']])

    data3 = frame.take(2, offset=2, columns=['name', 'tenure'])
    assert(data3 == [['Thurston', 26], ['Judy', 14]])

    data4 = frame.take(0, offset=2, columns=['name', 'tenure'])
    assert(data4 == [])


def test_take_scala_backend(tc):
    frame = _make_frame(tc)
    frame._scala
    data1 = frame.take(2, columns=['name', 'phone'])
    assert(data1 == [[u'Fred', u'555-1234'], [u'Susan', u'555-0202']])

    data2 = frame.take(2, offset=2)
    assert(data2 == [[u'Thurston', 65, 26, u'555-4510'], [u'Judy', 44, 14, u'555-2183']])

    data3 = frame.take(2, offset=2, columns=['name', 'tenure'])
    assert(data3 == [[u'Thurston', 26], [u'Judy', 14]])

    data4 = frame.take(0, offset=2, columns=['name', 'tenure'])
    assert(data4 == [])


def test_take_python_backend_negative(tc):
    frame = _make_frame(tc)
    try:
        frame.take(-1)
    except ValueError:
        pass
    else:
        raise RuntimeError("expected bad arugment error")
    try:
        frame.take(3, offset=-10)
    except ValueError:
        pass
    else:
        raise RuntimeError("expected bad arugment error")


def test_take_scala_backend_negative(tc):
    frame = _make_frame(tc)
    frame._scala
    try:
        frame.take(-1)
    except ValueError:
        pass
    else:
        raise RuntimeError("expected bad arugment error")
    try:
        frame.take(3, offset=-10)
    except ValueError:
        pass
    else:
        raise RuntimeError("expected bad arugment error")

