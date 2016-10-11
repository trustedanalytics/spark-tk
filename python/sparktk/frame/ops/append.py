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