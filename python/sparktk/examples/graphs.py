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

from sparktk import TkContext as _TkContext  # use underscore to hide from public namespace

_movie_graph = None

def get_movie_graph(tc=_TkContext.implicit):
    _TkContext.validate(tc)
    global _movie_graph
    if _movie_graph is None:
        viewers = tc.frame.create([['fred', 0],
                                   ['wilma', 0],
                                   ['pebbles', 1],
                                   ['betty', 0],
                                   ['barney', 0],
                                   ['bamm bamm', 1]],
                                  schema=[('id', str), ('kids', int)])

        titles = ['Croods', 'Jurassic Park', '2001', 'Ice Age', 'Land Before Time']

        movies = tc.frame.create([[t] for t in titles], schema=[('id', str)])

        vertices = viewers.copy()
        vertices.append(movies)

        edges = tc.frame.create([['fred','Croods',5],
                                 ['fred','Jurassic Park',5],
                                 ['fred','2001',2],
                                 ['fred','Ice Age',4],
                                 ['wilma','Jurassic Park',3],
                                 ['wilma','2001',5],
                                 ['wilma','Ice Age',4],
                                 ['pebbles','Croods',4],
                                 ['pebbles','Land Before Time',3],
                                 ['pebbles','Ice Age',5],
                                 ['betty','Croods',5],
                                 ['betty','Jurassic Park',3],
                                 ['betty','Land Before Time',4],
                                 ['betty','Ice Age',3],
                                 ['barney','Croods',5],
                                 ['barney','Jurassic Park',5],
                                 ['barney','Land Before Time',3],
                                 ['barney','Ice Age',5],
                                 ['bamm bamm','Croods',5],
                                 ['bamm bamm','Land Before Time',3]],
                                schema=['src', 'dst', 'rating'])

        _movie_graph = tc.graph.create(vertices, edges)

    return _movie_graph
