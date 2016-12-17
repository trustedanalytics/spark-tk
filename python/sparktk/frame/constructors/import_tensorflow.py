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

from sparktk import TkContext


def import_tensorflow(source_tf_path, schema=None, tc=TkContext.implicit):
    """

    :param source_tf_path:
    :param tc:
    :param schema:
    :return:
    """

    scala_frame = tc.sc._jvm.org.trustedanalytics.sparktk.frame.internal.constructors.ImportTensorflow.importTensorflow(tc._scala_sc, source_tf_path, schema)

    from sparktk.frame.frame import Frame
    return Frame(tc, scala_frame)