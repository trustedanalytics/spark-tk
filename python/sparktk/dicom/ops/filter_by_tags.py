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

def filter_by_tags(self, dict_tags_values):
    """
    filter metadata with given dictionary of tags and values

     Ex: dict_tags_values -> {"00100020":"12344", "00080018","1.3.12.2.1107.5.2.5.11090.5.0.5823667428974336"}

    :param dict_tags_values: (dict(str, str)) dictionary of tags and values from xml string in metadata

    """