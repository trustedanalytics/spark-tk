def append(self, frame):
    """
    Adds more data to the current frame.

    Parameters
    ----------

    :param frame: (Frame) Frame of data to append to the current frame.

    Examples
    --------

    In this example, we start off by creating a frame of animals.

        >>> animals = tc.frame.create([['dog', 'snoopy'],['cat', 'tom'],['bear', 'yogi'],['mouse', 'jerry']],
        ...                       [('animal', str), ('name', str)])
        <progress>

        >>> animals.inspect()
        [#]  animal  name
        ===================
        [0]  dog     snoopy
        [1]  cat     tom
        [2]  bear    yogi
        [3]  mouse   jerry

    Then, we append a frame that will add a few more animals to the original frame.

        >>> animals.append(tc.frame.create([['donkey'],['elephant'], ['ostrich']], [('animal', str)]))
        <progress>

        >>> animals.inspect()
        [#]  animal    name
        =====================
        [0]  dog       snoopy
        [1]  cat       tom
        [2]  bear      yogi
        [3]  mouse     jerry
        [4]  donkey    None
        [5]  elephant  None
        [6]  ostrich   None


    The data we added didn't have names, so None values were inserted for the new rows.

    """
    from sparktk.frame.frame import Frame
    if not isinstance(frame, Frame):
        raise TypeError("frame must be a Frame type, but is: {0}".format(type(frame)))
    self._scala.append(frame._scala)