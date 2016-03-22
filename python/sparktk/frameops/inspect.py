from sparktk.inspect import RowsInspection, inspect_settings

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


    Examples
    --------
    To look at the first 4 rows of data in a frame:

    .. code::

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

    .. code::

    <skip>
        >>> print unicode(frame.inspect())
    </skip>


    **Global Settings**

    If not specified, the arguments that control formatting receive default values from
    'trustedanalytics.inspect_settings'.  Make changes there to affect all calls to inspect.

    .. code::

        >>> import trustedanalytics as ta
        >>> ta.inspect_settings
        wrap             20
        truncate       None
        round          None
        width            80
        margin         None
        with_types    False
        >>> ta.inspect_settings.width = 120  # changes inspect to use 120 width globally
        >>> ta.inspect_settings.truncate = 16  # changes inspect to always truncate strings to 16 chars
        >>> ta.inspect_settings
        wrap             20
        truncate         16
        round          None
        width           120
        margin         None
        with_types    False
        >>> ta.inspect_settings.width = None  # return value back to default
        >>> ta.inspect_settings
        wrap             20
        truncate         16
        round          None
        width            80
        margin         None
        with_types    False
        >>> ta.inspect_settings.reset()  # set everything back to default
        >>> ta.inspect_settings
        wrap             20
        truncate       None
        round          None
        width            80
        margin         None
        with_types    False

    ..
    """
    format_settings = inspect_settings.copy(wrap, truncate, round, width, margin, with_types)
    result = self.take(n) #, offset, selected_columns)
    data = result.data
    schema = result.schema
    return RowsInspection(data, schema, offset=offset, format_settings=format_settings)
