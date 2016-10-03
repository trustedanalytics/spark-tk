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

    data5 = frame.take(10)
    assert(data5 == [[u'Fred', 39, 16, u'555-1234'], [u'Susan', 33, 3, u'555-0202'], [u'Thurston', 65, 26, u'555-4510'], [u'Judy', 44, 14, u'555-2183']])

    data6 = frame.take(3, offset=3)
    assert(data6 == [[u'Judy', 44, 14, u'555-2183']])

    data7 = frame.take(3, offset=3, columns=['name', 'tenure'])
    assert(data7 == [['Judy', 14]])

    data8 = frame.take(2, offset=6, columns=['name', 'tenure'])  # offset beyond
    assert(data8 == [])


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

    data5 = frame.take(10)
    assert(data5 == [[u'Fred', 39, 16, u'555-1234'], [u'Susan', 33, 3, u'555-0202'], [u'Thurston', 65, 26, u'555-4510'], [u'Judy', 44, 14, u'555-2183']])

    data6 = frame.take(3, offset=3)
    assert(data6 == [[u'Judy', 44, 14, u'555-2183']])

    data7 = frame.take(3, offset=3, columns=['name', 'tenure'])
    assert(data7 == [['Judy', 14]])

    data8 = frame.take(2, offset=6, columns=['name', 'tenure'])  # offset beyond
    assert(data8 == [])


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

