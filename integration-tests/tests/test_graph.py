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
from sparktk.graph.graph import Graph


def get_friends_gf(tc):
    from graphframes import examples
    return examples.Graphs(tc.sql_context).friends()


def test_create_graph_from_graphframe(tc):
    gf = get_friends_gf(tc)
    graph_from_py = Graph(tc, gf)
    s_from_py = str(graph_from_py.graphframe)
    graph_from_scala = Graph(tc, gf._jvm_graph)
    s_from_scala = str(graph_from_scala.graphframe)
    assert(s_from_py == s_from_scala)
    assert(graph_from_py.graphframe.vertices.count() == graph_from_scala.graphframe.vertices.count())


def test_create_graph_from_frames(tc):
    v = tc.frame.create([[0, 'zero'], [1, 'one'], [2, 'two'], [3, 'three'], [4, 'four'], [5, 'five']],
                        [('id', int), ('s', str)])
    e = tc.frame.create([[0, 1], [1, 2], [1, 3], [2, 3], [4, 5], [5, 1]],
                        [('src', int), ('dst', int)])
    g = Graph(tc, v, e)
    assert(str(g) == 'Graph(v:[id: int, s: string], e:[src: int, dst: int])')


def test_create_graph_from_frames_negative(tc):
    v_good = tc.frame.create([[0, 'zero'], [1, 'one'], [2, 'two'], [3, 'three'], [4, 'four'], [5, 'five']],
                             [('id', int), ('s', str)])
    e_good = tc.frame.create([[0, 1], [1, 2], [1, 3], [2, 3], [4, 5], [5, 1]],
                             [('src', int), ('dst', int)])
    v_bad = tc.frame.create([[0, 'zero'], [1, 'one'], [2, 'two'], [3, 'three'], [4, 'four'], [5, 'five']],
                             [('number', int), ('s', str)])
    e_bad = tc.frame.create([[0, 1], [1, 2], [1, 3], [2, 3], [4, 5], [5, 1]],
                             [('src', int), ('dusty', int)])
    try:
        g = Graph(tc, v_bad, e_good)
    except ValueError as error:
        print str(error)
        assert("column 'id' missing" in str(error))
    else:
        raise Exception("Expected ValueError for missing 'id' column but passed")

    try:
        g = Graph(tc, v_good, e_bad)
    except ValueError as error:
        print str(error)
        assert("column 'dst' missing" in str(error))
    else:
        raise Exception("Expected ValueError for missing 'dst' column but passed")

