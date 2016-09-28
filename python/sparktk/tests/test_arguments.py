import unittest

from sparktk.arguments import affirm_type, require_type, implicit


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


if __name__ == '__main__':
    unittest.main()