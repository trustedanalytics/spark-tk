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

from setup import tc, rm, get_sandbox_path

def test_append_to_empty_frame(tc):
    frame1 = tc.frame.create([],[])
    assert(frame1.count() == 0)
    assert(frame1.column_names == [])
    frame2 = tc.frame.create([[1],[2],[3]], [("numbers", int)])

    # append empty frame to the populated frame
    frame2.append(frame1)
    assert(frame2.count() == 3)
    assert(frame2.column_names == ["numbers"])

    # append populated frame to empty frame
    frame1.append(frame2)
    assert(frame1.count() == 3)
    assert(frame1.column_names == ["numbers"])

def test_append_new_columns(tc):
    two_columns = [("number", int), ("string", str)]
    frame = tc.frame.create([[i,str(i)] for i in range(1,21)], two_columns)
    assert(frame.count() == 20)
    three_columns = [("number", int),("string", str),("float", float)]
    frame.append(tc.frame.create([[i,str(i),float(i)] for i in range(21,31)],three_columns))
    assert(frame.count() == 30)
    assert(frame.column_names == ["number", "string", "float"])
    values = frame.take(frame.count())
    # The first 20 rows should have the int and string, but None in the float column
    for i in range(0,20):
        assert(values[i][0] == i+1)
        assert(values[i][1] == str(i+1))
        assert(values[i][2] == None)
    # The last 10 rows should have all three columns populated
    for i in range(21,30):
        assert(values[i][0] == i+1)
        assert(values[i][1] == str(i+1))
        assert(values[i][2] == float(i+1))