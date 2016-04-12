import unittest

import sparktk.lazyloader as ll

def multiply_for_lazy_loader(a, b):
    return a * b

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


if __name__ == '__main__':
    unittest.main()
