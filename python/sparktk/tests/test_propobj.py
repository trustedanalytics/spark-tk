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

import unittest

from sparktk.propobj import PropertiesObject

# Define a class hierarchy A->B->C to test inheritance with PropertiesObject


class A(PropertiesObject):

    def __init__(self, w, x):
        self.w = w
        self._x = x

    @property
    def x(self):
        return self._x

    @property
    def t(self):
        return "A*"


class B(A):

    def __init__(self, w, x, y, z):
        self.y = y
        self._z = z
        super(B, self).__init__(w, x)

    @property
    def z(self):
        return self._z

    @property
    def t(self):
        return "B*"


class C(B):

    def __init__(self, w, x, y, z):
        super(C, self).__init__(w, x, y, z)

    @property
    def x(self):
        return 10  # intentionally override x


class TestPropertiesObject(unittest.TestCase):

    def test_simple_repr(self):
        a = A(1, 2)
        expected = """t = A*
w = 1
x = 2"""
        self.assertEquals(expected, repr(a))

    def test_inherited_repr(self):
        b = B(1, 2, 3, 4)
        expected = """t = B*
w = 1
x = 2
y = 3
z = 4"""
        self.assertEquals(expected, repr(b))

    def test_get_class_items_handles_overrides(self):
        c = C(1, 2, 3, 4)
        expected = """t = B*
w = 1
x = 10
y = 3
z = 4"""  # t = B* because we DON'T override it in C, but B overrides it in A
        self.assertEquals(expected, repr(c))


if __name__ == '__main__':
    unittest.main()
