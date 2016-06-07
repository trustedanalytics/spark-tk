import unittest

import sparktk.lazyloader as ll
from sparktk.lazyloader import implicit


def multiply_for_lazy_loader(a, b):
    return a * b


def multiply_with_implicit_kwarg(a, b, base=implicit):
    return base(a*b)


class TestLazyLoader(unittest.TestCase):

    def test_get_module_name(self):
        self.assertEqual(ll.get_module_name("/home/user1/python/sparko/bunch/grapes", 'sparko'), 'sparko.bunch.grapes')
        self.assertEqual(ll.get_module_name("/home/user1/python/sparko", 'sparko'), 'sparko')
        try:
            ll.get_module_name("/home/user1/jungle/gym/grapes", 'sparko')
        except ValueError as e:
            self.assertEqual("package_name sparko not found in path /home/user1/jungle/gym/grapes", str(e))
            pass
        else:
            self.fail("Expected ValueError for bad path-package combo")

    def test_get_private_name(self):
        self.assertEqual(ll.name_to_private("jam"), "_jam")

    def test_is_public_python_name(self):
        self.assertTrue(ll.is_public_python_name('clustering'))
        self.assertTrue(ll.is_public_python_name('Club5'))
        self.assertFalse(ll.is_public_python_name('_clustering'))
        self.assertFalse(ll.is_public_python_name('1clustering'))
        self.assertFalse(ll.is_public_python_name('-means'))
        self.assertFalse(ll.is_public_python_name('k-means'))
        self.assertFalse(ll.is_public_python_name('k means'))
        self.assertTrue(ll.is_public_python_name('k'))
        self.assertFalse(ll.is_public_python_name('1'))
        self.assertFalse(ll.is_public_python_name('_'))
        self.assertFalse(ll.is_public_python_name(''))
        try:
            ll.is_public_python_name(None)
        except ValueError as e:
            self.assertEqual("Expected string value, got None", str(e))
        else:
            self.fail("Expected ValueError for None")

    def test_get_lazy_loader(self):
        class Dummy(object):
            pass
        dummy = Dummy()
        loader = ll.get_lazy_loader(dummy, "tests")  # use the sparktk tests subpackage
        self.assertTrue(isinstance(loader, ll.LazyLoader))
        # see if it has a property for this module
        self.assertEqual('SparktkTestsTest_lazyloaderLazyLoader', type(loader.test_lazyloader).__name__)
        # see if we can call the module level multiply method
        self.assertEqual(loader.test_lazyloader.multiply_for_lazy_loader(3, 4), 12)
        # see if class is the same class as if we imported it directly
        from sparktk.tests.test_lazyloader import TestLazyLoader as ImportedLoader
        self.assertTrue(loader.test_lazyloader.TestLazyLoader is ImportedLoader)

    def test_get_lazy_loader_with_implicit_kwargs(self):
        class Dummy(object):
            pass
        dummy = Dummy()

        def base10(x):
            return "base10(%s)" % x

        loader = ll.get_lazy_loader(dummy, "tests", implicit_kwargs={'base': base10})  # specify implicit value
        self.assertTrue(isinstance(loader, ll.LazyLoader))
        # see if it has a property for this module
        self.assertEqual('SparktkTestsTest_lazyloaderLazyLoader', type(loader.test_lazyloader).__name__)
        # see if we can call the module level multiply method
        self.assertEqual(loader.test_lazyloader.multiply_with_implicit_kwarg(5, 7), "base10(35)")

    def test_wrap_for_implicit_kwargs(self):
        from sparktk.lazyloader import wrap_for_implicit_kwargs, implicit

        def subject(a, b, c=implicit, d=4):
            """I am the subject"""
            if c is implicit:
                implicit.error('c')
            return ':'.join([str(a), str(b), str(c), str(d)])

        wrap1 = wrap_for_implicit_kwargs(subject, {'c': 'three'})
        self.assertEqual("I am the subject", wrap1.__doc__)

        result = wrap1(1, 2)
        print "result=%s" % result
        self.assertEqual('1:2:three:4', result)
        result = wrap1(1, 2, c='override')
        self.assertEqual('1:2:override:4', result)

        # try implicitly setting 'd' which is not marked as implicit
        try:
            wrap_for_implicit_kwargs(subject, {'d': 'four'})
        except TypeError as e:
            self.assertEqual("Lazyloader asked to implicitly fill arg 'd' but it is not marked implicit in function subject", str(e))
        else:
            self.fail("Expected TypeError for checking implicit function arg which is not marked implicit")

        # try calling subject without an implicit fill
        try:
            subject(1, 2)
        except ValueError as e:
            self.assertTrue("Missing value for arg 'c'." in str(e))
        else:
            self.fail("Expected ValueError for not filling a variable marked implicit")

if __name__ == '__main__':
    unittest.main()
