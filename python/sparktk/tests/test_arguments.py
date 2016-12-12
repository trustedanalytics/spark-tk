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

from sparktk.arguments import affirm_type, require_type, implicit
import sparktk.arguments as arguments


class TestAffirmType(unittest.TestCase):

    def test_list_of_str(self):
        x = ["uno", "dos", "tres"]
        result = affirm_type.list_of_str(x, "a")
        self.assertEqual(x, result)

    def test_list_of_str_single_string(self):
        x = "uno"
        result = affirm_type.list_of_str(x, "a")
        self.assertEqual([x], result)

    def test_list_of_str_type_error(self):
        x = 3.14
        try:
            affirm_type.list_of_str(x, "a")
        except TypeError as e:
            msg = str(e)
            expected = "Expected type str or list of str."
            self.assertTrue(expected in msg, "\nexpected=%s\nmessage =%s" % (expected, msg))
        else:
            self.fail("A TypeError should have been raised")

    def test_list_of_str_value_error(self):
        x = [1, 2, 3]
        try:
            affirm_type.list_of_str(x, "a")
        except ValueError as e:
            msg = str(e)
            expected = "Expected str or list of str"
            self.assertTrue(expected in msg, "\nexpected=%s\nmessage =%s" % (expected, msg))
        else:
            self.fail("A ValueError should have been raised")

    def test_list_of_str_bad_len(self):
        try:
           affirm_type.list_of_str(["a", "b", "c"], "a", length=2)
        except ValueError as e:
            msg = str(e)
            expected = "Expected list of str of length 2."
            self.assertTrue(msg.endswith(expected), "expected error message should have ended with '%s', message =%s" % (expected, msg))
        else:
            self.fail("A ValueError should have been raised")

    def test_list_of_float(self):
        list1 = [1.0, 3.14]
        result = affirm_type.list_of_float(list1, "a")
        self.assertEqual(list1, result)
        result = affirm_type.list_of_float(list1, "a", length=2)
        self.assertEqual(list1, result)
        result = affirm_type.list_of_float([1.0, 3, '5'], "a")
        self.assertEqual([1.0, 3.0, 5.0], result)

    def test_list_of_float_single_float(self):
        x = 6.28
        result = affirm_type.list_of_float(x, "a")
        self.assertEqual([x], result)

        x = 6.28
        result = affirm_type.list_of_float(x, "a", length=1)
        self.assertEqual([x], result)

        x = 10  # integer
        result = affirm_type.list_of_float(x, "a")
        self.assertEqual([10.0], result)

    def test_list_of_float_value_error(self):
        x = [2, "whoops", 4]
        try:
            affirm_type.list_of_float(x, "a")
        except ValueError as e:
            msg = str(e)
            expected = "Expected list of float"
            self.assertTrue(expected in msg, "\nexpected=%s\nmessage =%s" % (expected, msg))
        else:
            self.fail("A ValueError should have been raised")

    def test_list_of_float_bad_len(self):
        try:
           affirm_type.list_of_float([1.0, 2.0, 5.0], "a", length=4)
        except ValueError as e:
            msg = str(e)
            expected = "Expected list of float of length 4."
            self.assertTrue(msg.endswith(expected), "expected error message should have ended with '%s', message =%s" % (expected, msg))
        else:
            self.fail("A ValueError should have been raised")

class TestRequireType(unittest.TestCase):

    def test_basic(self):
        require_type(int, 1, "a")
        require_type(str, "1", "a")
        require_type(list, [1, 2, 3], "a")

    def test_basic_negative(self):
        try:
            require_type(int, "12", "a")
        except TypeError as e:
            msg = str(e)
            expected = "Expected type <type 'int'>"
            self.assertTrue(expected in msg, "\nexpected=%s\nmessage =%s" % (expected, msg))
        else:
            self.fail("A TypeError should have been raised")

    def test_implicit(self):
        try:
            require_type(int, implicit, "a")
        except ValueError as e:
            self.assertEqual("Missing value for arg 'a'.  This value is normally filled implicitly, however, if this method is called standalone, it must be set explicitly", str(e))
        else:
            self.fail("A ValueError should have been raised")

    def test_non_negative_int(self):
        require_type.non_negative_int(1, "a")

    def test_non_negative_int_value_error(self):
        try:
            require_type.non_negative_int(-1, "a")
        except ValueError as e:
            msg = str(e)
            expected = "Expected non-negative integer"
            self.assertTrue(expected in msg, "\nexpected=%s\nmessage =%s" % (expected, msg))
        else:
            self.fail("A ValueError should have been raised")

    def test_non_negative_int_type_error(self):
        try:
            require_type.non_negative_int("12", "a")
        except TypeError as e:
            msg = str(e)
            expected = "Expected type <type 'int'>"
            self.assertTrue(expected in msg, "\nexpected=%s\nmessage =%s" % (expected, msg))
        else:
            self.fail("A TypeError should have been raised")

    def test_non_empty_str(self):
        require_type.non_empty_str("something", "a")

    def test_non_empty_str_type_error(self):
        try:
            require_type.non_empty_str(100, "a")
        except TypeError as e:
            msg = str(e)
            expected = "Expected type <type 'str'>"
            self.assertTrue(expected in msg, "\nexpected=%s\nmessage =%s" % (expected, msg))
        else:
            self.fail("A TypeError should have been raised")

    def test_non_empty_str_value_error(self):
        extra_message = "this is the end."
        try:
            require_type.non_empty_str('', "a", extra_message)
        except ValueError as e:
            msg = str(e)
            expected = "Expected non-empty string"
            self.assertTrue(expected in msg, "\nexpected=%s\nmessage =%s" % (expected, msg))
            self.assertTrue(msg.endswith(extra_message), "message should have ended with '%s', but got '%s'" % (extra_message, msg))
        else:
            self.fail("A ValueError should have been raised")

def f0():
    pass

def f1(a, b):
    pass

def f2(a, b=4):
    pass

def f3(a=True, _b=3.14):
    pass

def f4(a, **bonus):
    pass

def f5(a, *variable):
    pass

def f6(*one, **two):
    pass

def f7(a, b, *c, **d):
    pass

def f8(uno, dos=2, tres=3, **otro):
    pass

def fself(self, surf, turf=100):
    pass


class TestReflection(unittest.TestCase):

    def test_is_private_name(self):
        self.assertTrue(arguments.is_name_private("_secret"))
        self.assertFalse(arguments.is_name_private("nice"))
        self.assertFalse(arguments.is_name_private("_public_underscore", public=["_public_underscore"]))
        self.assertTrue(arguments.is_name_private("_public_whoops", public= ["_public", "_popular"]))

    def test_default_value_to_str(self):
        self.assertEqual('None', str(arguments.default_value_to_str(None)))
        self.assertEqual('5', str(arguments.default_value_to_str(5)))
        self.assertEqual('False', str(arguments.default_value_to_str(False)))
        self.assertEqual("'Soup of the Day'", str(arguments.default_value_to_str('Soup of the Day')))

    def test_get_args_spec_from_function(self):
        self.assertEqual(([], [], None, None), arguments.get_args_spec_from_function(f0))
        self.assertEqual((['a', 'b'], [], None, None), arguments.get_args_spec_from_function(f1))
        self.assertEqual((['a'], [('b', 4)], None, None), arguments.get_args_spec_from_function(f2))
        self.assertEqual(([], [('a', True), ('_b', 3.14)], None, None), arguments.get_args_spec_from_function(f3))
        self.assertEqual(([], [('a', True)], None, None), arguments.get_args_spec_from_function(f3, ignore_private_args=True))
        self.assertEqual((['a'], [], None, 'bonus'), arguments.get_args_spec_from_function(f4))
        self.assertEqual((['a'], [], 'variable', None), arguments.get_args_spec_from_function(f5))
        self.assertEqual(([], [], 'one','two'), arguments.get_args_spec_from_function(f6))
        self.assertEqual((['a', 'b'], [], 'c', 'd'), arguments.get_args_spec_from_function(f7))
        self.assertEqual((['uno'], [('dos', 2), ('tres', 3)], None, 'otro'), arguments.get_args_spec_from_function(f8))
        self.assertEqual((['self', 'surf'], [('turf', 100)], None, None), arguments.get_args_spec_from_function(fself))
        self.assertEqual((['surf'], [('turf', 100)], None, None), arguments.get_args_spec_from_function(fself, ignore_self=True))

    def test_get_args_text_from_function(self):
        self.assertEqual("", arguments.get_args_text_from_function(f0))
        self.assertEqual("a, b", arguments.get_args_text_from_function(f1))
        self.assertEqual("a, b=4", arguments.get_args_text_from_function(f2))
        self.assertEqual("a=True, _b=3.14", arguments.get_args_text_from_function(f3))
        self.assertEqual("a=True", arguments.get_args_text_from_function(f3, ignore_private_args=True))
        self.assertEqual("a, **bonus", arguments.get_args_text_from_function(f4))
        self.assertEqual("a, *variable", arguments.get_args_text_from_function(f5))
        self.assertEqual("*one, **two", arguments.get_args_text_from_function(f6))
        self.assertEqual("a, b, *c, **d", arguments.get_args_text_from_function(f7))
        self.assertEqual("uno, dos=2, tres=3, **otro", arguments.get_args_text_from_function(f8))
        self.assertEqual("self, surf, turf=100", arguments.get_args_text_from_function(fself))
        self.assertEqual("surf, turf=100", arguments.get_args_text_from_function(fself, ignore_self=True))

    def test_validate_call(self):
        arguments.validate_call(f0, {})
        arguments.validate_call(f1, {'a': 3, 'b': 4})
        arguments.validate_call(f2, {'a': True})
        arguments.validate_call(f2, {'a': True, 'b': 99})
        arguments.validate_call(f3, {'a': False, '_b': 8.2})
        arguments.validate_call(f4, {'other': 1 , 'a': False, 'whatever': 34})
        arguments.validate_call(f8, {'uno': 1})
        arguments.validate_call(f8, {'ocho': 8, 'uno': 1})

    def test_validate_call_neg(self):
        def unknown_args(function, parameters, unexpected_args_str):
            try:
                arguments.validate_call(function, parameters)
            except ValueError as e:
                self.assertTrue("included one or more unknown args named: %s" % unexpected_args_str in str(e), str(e))
            else:
                self.fail("Negative test failure: expected ValueError when validating function %s called with args: %s"
                          % (function.__name__, parameters))

        def missing_args(function, parameters, missing_args_str):
            try:
                arguments.validate_call(function, parameters)
            except ValueError as e:
                self.assertTrue("is missing the following required arguments: %s" % missing_args_str in str(e), str(e))
            else:
                self.fail("Negative test failure: expected ValueError when validating function %s called with args: %s"
                          % (function.__name__, parameters))

        unknown_args(f0, {'a': 'barrel'}, 'a')
        unknown_args(f1, {'b': 4, 'c': 5}, 'c')
        missing_args(f1, {'b': 4}, 'a')
        missing_args(f2, {'b': 4}, 'a')
        unknown_args(f3, {'c': 4, 'b': 5}, 'c')
        missing_args(f4, {}, 'a')
        missing_args(f4, {'x': 89}, 'a')
        missing_args(f8, {'x': 89}, 'uno')

    def test_validate_call_neg_varargs(self):
        try:
            arguments.validate_call(f5, {})
        except ValueError as e:
            self.assertEqual("function f5(a, *variable) cannot be validated against a dict of parameters because of the '*variable' in its signature",
                             str(e))
        else:
            self.fail("Negative test failure: expected ValueError when validating function with varargs")

        try:
            arguments.validate_call(f7, {'x': 'junk'})
        except ValueError as e:
            self.assertEqual("function f7(a, b, *c, **d) cannot be validated against a dict of parameters because of the '*c' in its signature",
                             str(e))
        else:
            self.fail("Negative test failure: expected ValueError when validating function with varargs")


if __name__ == '__main__':
    unittest.main()