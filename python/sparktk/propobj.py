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

import json


class PropertiesObject(object):
    """Simple object which provides nice repr, to_dict, etc. for attributes and properties"""

    def to_dict(self):
        d = self._properties()
        d.update(self._attributes())
        return d

    def to_json(self):
        return json.dumps(self.to_dict())

    def __repr__(self):
        d = self.to_dict()
        max_len = 0
        for k in d.keys():
            max_len = max(max_len, len(k))
        return "\n".join(["%s = %s" % (self._pad_right(k, max_len), str(d[k])) for k in sorted(d.keys())])

    def _attributes(self):
        return dict([(k, v) for k, v in self.__dict__.items() if not k.startswith('_')])

    def _properties(self):
        class_items = self.__class__.__dict__.iteritems()
        return dict([(k, getattr(self, k)) for k, v in class_items if isinstance(v, property)])

    @staticmethod
    def _pad_right(s, target_len):
        """pads string s on the right such that is has at least length target_len"""
        return s + ' ' * (target_len - len(s))
