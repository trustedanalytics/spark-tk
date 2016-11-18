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

from sparktk.dtypes import dtypes


class JConvert(object):
    """Handles misc. conversions going to and from Scala for simple non-primitive types, like lists, option, etc."""

    def __init__(self, jutils):
        self.jutils = jutils
        self.sc = jutils.sc
        self.scala = self.sc._jvm.org.trustedanalytics.sparktk.jvm.JConvert

    def list_to_double_list(self, python_list):
        return [float(item) for item in python_list]

    def to_scala_list_double(self, python_list):
        return self.scala.toScalaList(self.list_to_double_list(python_list))

    def to_scala_list(self, python_list):
        return self.scala.toScalaList(python_list)

    def to_scala_list_string(self, python_list):
        return self.scala.toScalaList([unicode(item) for item in python_list])

    def to_scala_list_string_bool_tuple(self, python_list):
        return self.scala.toScalaList([self.scala.toScalaTuple2(unicode(item[0]), item[1]) for item in python_list])

    def to_scala_list_string_option_tuple(self, python_list):
        return self.scala.toScalaList([self.scala.toScalaTuple2(unicode(item[0]), self.scala.toOption(item[1])) for item in python_list])

    def to_scala_vector_double(self, python_list):
        return self.scala.toScalaVector(self.list_to_double_list(python_list))

    def to_scala_vector_string(self, python_list):
        return self.scala.toScalaVector([unicode(item) for item in python_list])

    def to_scala_map(self, python_dict):
        return self.scala.toScalaMap(python_dict)

    def scala_map_to_python_with_iterable_values(self, m):
        result = self.scala_map_to_python(m)
        python_map_with_iterable_values = {}
        for k,v in result.items():
            python_map_with_iterable_values[k] = list(self.scala.scalaSeqToPython(v))
        return python_map_with_iterable_values

    def scala_map_to_python(self, m):
        return self.scala.scalaMapToPython(m)

    def scala_option_map_to_python(self, m):
        python_option = self.from_scala_option(m)
        return None if not python_option else self.scala_map_to_python(python_option)

    def to_scala_option_map(self, m):
        if isinstance(m, dict):
            scala_map = self.to_scala_map(m)
        elif m is None:
            scala_map = None
        else:
            raise ValueError("Expected a dict object, but received: %s" % type(m))
        return self.scala.toOption(scala_map)

    def to_scala_option(self, item):
        return self.scala.toOption(item)

    def to_scala_option_list_double(self, python_list):
        if isinstance(python_list, list):
            python_list = self.list_to_double_list(python_list)
        return self.to_scala_option(python_list)

    def to_scala_option_list_string(self, python_list):
        if isinstance(python_list, list):
            python_list = self.to_scala_list_string(python_list)
        return self.to_scala_option(python_list)

    def to_scala_option_either_string_int(self, item):
        if item is not None:
            return self.scala.toOption(self.scala.toEitherStringInt(item))
        else:
            return self.scala.toOption(item)

    def to_scala_date_time_list(self, python_list):
        return self.scala.toScalaList([self.scala.toDateTime(item) for item in python_list])

    def to_scala_date_time(self, item):
        return self.scala.toDateTime(item)

    def from_scala_option(self, item):
        return self.scala.fromOption(item)

    def from_scala_seq(self, seq):
        return self.scala.scalaSeqToPython(seq)

    def from_scala_vector(self, vector):
        return list(self.scala.scalaVectorToPython(vector))

    def from_scala_matrix(self, matrix):
        result = self.scala.scalaMatrixToPython(matrix)
        python_list = list(result)
        return [list(py_list) for py_list in python_list]

    def to_scala_group_by_aggregation_args(self, python_map):
        scala_map = self.to_scala_map(python_map)
        return self.sc._jvm.org.trustedanalytics.sparktk.frame.internal.ops.groupby.GroupByAggregationArgs(scala_map)

    def combine_scala_maps(self, python_list_of_scala_maps):
        scala_list = self.to_scala_list(python_list_of_scala_maps)
        return self.scala.combineScalaMap(scala_list)