import unittest

from sparktk.frame.schema import get_indices_for_selected_columns


class TestSchema(unittest.TestCase):

    def test_get_indices_for_selected_columns(self):
        results = get_indices_for_selected_columns([("a", int),
                                                    ("b", int),
                                                    ("c", int),
                                                    ("d", int),
                                                    ("e", int),
                                                    ("f", int)], ['b', 'e', 'f'])
        print results



if __name__ == '__main__':
    unittest.main()