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

from sparktk.atable import ATable, Formatting

inspect_settings = Formatting()

def inspect(self,
            n=10,
            offset=0,
            columns=None,
            wrap=inspect_settings._unspecified,
            truncate=inspect_settings._unspecified,
            round=inspect_settings._unspecified,
            width=inspect_settings._unspecified,
            margin=inspect_settings._unspecified,
            with_types=inspect_settings._unspecified):
    """
    Pretty-print of the frame data

    Essentially returns a string, but technically returns a RowInspection object which renders a string.
    The RowInspection object naturally converts to a str when needed, like when printed or when displayed
    by python REPL (i.e. using the object's __repr__).  If running in a script and want the inspect output
    to be printed, then it must be explicitly printed, then `print frame.inspect()`

    Parameters
    ----------
    :param n: (Optional[int]) The number of rows to print
    :param offset: (Optional[int]) The number of rows to skip before printing.
    :param columns: (Optional[List[str]]) Filter columns to be included.  By default, all columns are included.
    :param wrap: (Optional[int or 'stripes']) If set to 'stripes' then inspect prints rows in stripes; if set to an
                 integer N, rows will be printed in clumps of N columns, where the columns are wrapped.
    :param truncate: (Optional[int]) If set to integer N, all strings will be truncated to length N, including all
                     tagged ellipses.
    :param round: (Optional[int]) If set to integer N, all floating point numbers will be rounded and truncated to
                  N digits.
    :param width: (Optional[int]) If set to integer N, the print out will try to honor a max line width of N.
    :param margin: (Optional[int]) Applies to 'stripes' mode only.  If set to integer N, the margin for printing names
                   in a stripe will be limited to N characters.
    :param with_types: (Optinoal[bool]) If set to True, header will include the data_type of each column.
    :return: (RowsInspection) An object which naturally converts to a pretty-print string.

    Examples
    --------
    To look at the first 4 rows of data in a frame:

    <skip>
        >>> frame.inspect(4)
        [#]  animal    name    age  weight
        ==================================
        [0]  human     George    8   542.5
        [1]  human     Ursula    6   495.0
        [2]  ape       Ape      41   400.0
        [3]  elephant  Shep      5  8630.0
    </skip>

    # For other examples, see :ref:`example_frame.inspect`.

    Note: if the frame data contains unicode characters, this method may raise a Unicode exception when
    running in an interactive REPL or otherwise which triggers the standard python repr().  To get around
    this problem, explicitly print the unicode of the returned object:

    <skip>
        >>> print unicode(frame.inspect())
    </skip>


    **Global Settings**

    If not specified, the arguments that control formatting receive default values from
    'sparktk.inspect_settings'.  Make changes there to affect all calls to inspect.

        >>> import sparktk
        >>> sparktk.inspect_settings
        wrap             20
        truncate       None
        round          None
        width            80
        margin         None
        with_types    False
        >>> sparktk.inspect_settings.width = 120  # changes inspect to use 120 width globally
        >>> sparktk.inspect_settings.truncate = 16  # changes inspect to always truncate strings to 16 chars
        >>> sparktk.inspect_settings
        wrap             20
        truncate         16
        round          None
        width           120
        margin         None
        with_types    False
        >>> sparktk.inspect_settings.width = None  # return value back to default
        >>> sparktk.inspect_settings
        wrap             20
        truncate         16
        round          None
        width            80
        margin         None
        with_types    False
        >>> sparktk.inspect_settings.reset()  # set everything back to default
        >>> sparktk.inspect_settings
        wrap             20
        truncate       None
        round          None
        width            80
        margin         None
        with_types    False

    """
    from sparktk.frame.ops.take import take_rich
    format_settings = inspect_settings.copy(wrap, truncate, round, width, margin, with_types)
    result = take_rich(self, n, offset, columns)
    return ATable(result.data, result.schema, offset=offset, format_settings=format_settings)
