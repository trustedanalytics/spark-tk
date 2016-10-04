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

import sparktk.frame.schema

from sparktk.frame.schema import schema_to_python, schema_to_scala

def rename_columns(self, names):
    """
    Rename columns

    Parameters
    ----------

    :param names: (dict) Dictionary of old names to new names.

    Examples
    --------
    Start with a frame with columns *Black* and *White*.

        <hide>

        >>> s = [('Black', unicode), ('White', unicode)]
        >>> rows = [["glass", "clear"],["paper","unclear"]]
        >>> my_frame = tc.frame.create(rows, s)
        -etc-

        </hide>

        >>> print my_frame.schema
        [('Black', <type 'unicode'>), ('White', <type 'unicode'>)]

    Rename the columns to *Mercury* and *Venus*:

        >>> my_frame.rename_columns({"Black": "Mercury", "White": "Venus"})

        >>> print my_frame.schema
        [(u'Mercury', <type 'unicode'>), (u'Venus', <type 'unicode'>)]

    """
    if not isinstance(names, dict):
        raise ValueError("Unsupported 'names' parameter type.  Expected dictionary, but found %s." % type(names))
    if self.schema is None:
        raise RuntimeError("Unable rename column(s), because the frame's schema has not been defined.")
    if self._is_python:
        scala_rename_map = self._tc.jutils.convert.to_scala_map(names)
        scala_schema = schema_to_scala(self._tc.sc, self._python.schema)
        rename_scala_schema = scala_schema.renameColumns(scala_rename_map)
        self._python.schema = schema_to_python(self._tc.sc, rename_scala_schema)
    else:
        self._scala.renameColumns(self._tc.jutils.convert.to_scala_map(names))
