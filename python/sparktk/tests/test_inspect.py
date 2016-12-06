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
import sparktk.atable as ui
import sparktk.dtypes as dtypes
from test_atable import TestATable

abc_schema = TestATable.abc_schema
two_abc_rows = TestATable.two_abc_rows
schema1 = TestATable.schema1
rows1 = TestATable.rows1



class TestInspect(unittest.TestCase):
    """Tests the ATable usage with things specific to sparktk, like vector types and numpy"""

    def test_round_vector(self):
        def r(n, value, num_digits):
            return ui.ATable.get_rounder(dtypes.vector(n), num_digits)(value)
        self.assertEqual("[1.23, 5.68]", r(4, [1.2345, 5.6789], 2))
        self.assertEqual("[1, 2, 3]", r(3, [1.234, 2.367, 3], 0))
        self.assertEqual("[1.2, 2.4, 3.0]", r(5, [1.23456, 2.36789, 3], 1))
        self.assertEqual("[1.23000, 2.36000, 3.00000]", r(2, [1.23, 2.36, 3], 5))

    def test_round_numpy(self):
        try:
            import numpy as np
        except ImportError:
            pass
        else:
            from collections import namedtuple
            T = namedtuple("T", ["expected", "value", "num_digits"])
            testcases = [T("3.14", 3.1415, 2),
                         T("6.28", 6.2830, 2),
                         T("867.53090", 867.5309, 5),
                         T("867.5309000000", 867.5309, 10),
                        ]

            def r32(t):
                return ui.ATable.get_rounder(np.float32, t.num_digits)(t.value)

            def r64(t):
                return ui.ATable.get_rounder(np.float64, t.num_digits)(t.value)

            for t in testcases:
                self.assertEqual(t.expected, r32(t))
                self.assertEqual(t.expected, r64(t))

    def test_inspect_round(self):
        schema = [('f32', float), ('f64', float), ('v', dtypes.vector(2))]
        rows = [[0.1234, 9.87654321, [1.0095, 2.034]],
                [1234.5, 9876.54321, [99.999, 33.33]]]
        result = repr(ui.ATable(rows, schema, offset=0, format_settings=ui.Formatting(wrap=2, round=2)))
        result = '\n'.join([line.rstrip() for line in result.splitlines()])
        expected = '''[#]  f32      f64      v
======================================
[0]     0.12     9.88  [1.01, 2.03]
[1]  1234.50  9876.54  [100.00, 33.33]'''
        self.assertEqual(expected, result)

        result = repr(ui.ATable(rows, schema, offset=0, format_settings=ui.Formatting(wrap='stripes', round=3)))
        result = '\n'.join([line.rstrip() for line in result.splitlines()])
        expected = '''[0]-
f32=0.123
f64=9.877
v  =[1.010, 2.034]
[1]-
f32=1234.500
f64=9876.543
v  =[99.999, 33.330]'''
        self.assertEqual(expected, result)

    def test_inspect_nones(self):
        schema = [('s', str), ('v', dtypes.vector(2))]
        rows = [['super', [1.0095, 2.034]],
                [None, None]]
        result = repr(ui.ATable(rows, schema, offset=0, format_settings=ui.Formatting(wrap=2, round=2, truncate=4)))
        result = '\n'.join([line.rstrip() for line in result.splitlines()])
        self.assertEqual("""[#]  s     v
=======================
[0]  s...  [1.01, 2.03]
[1]  None  None""", result)


if __name__ == '__main__':
    unittest.main()
