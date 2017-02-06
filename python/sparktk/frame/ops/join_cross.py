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

from sparktk.arguments import require_type

def join_cross(self, right):
    """
    join_cross performs a cross join operation on two frames and returns a frame that contains the cartesian product
    of the two frames.


    Parameters
    ----------

    :param right: (Frame) The right frame in the cross join operation.

    :returns: (Frame) A new frame with the results of the join

    The cross join operation returns a frame with the Cartesian product of the rows from the specified frames.  Each
    row from the current frame is combined with each row from the other frame.

    Notes
    -----
    The frame returned will contain all columns from the current frame and the right frame.  If a column name in the
    right frame already exists in the current frame, the column from the right frame will have a "_R" suffix.

    The order of columns after this method is called is not guaranteed.  It is recommended that you rename the columns
    to meaningful terms prior to using the join_cross method.

    Examples
    --------

    Start by creating two test frames to use with the cross join operation:

        >>> frame = tc.frame.create([[1],[2],[3]], [("id", int)])
        >>> frame.inspect()
        [#]  id
        =======
        [0]   1
        [1]   2
        [2]   3

        >>> right = tc.frame.create([["a"],["b"],["c"]], [("char", str)])
        >>> right.inspect()
        [#]  char
        =========
        [0]  a
        [1]  b
        [2]  c

    Perform a cross join on the frame with the right frame:

        >>> result = frame.join_cross(right)

    <hide>
        >>> result.sort(["id","char"])
    </hide>

        >>> result.inspect()
        [#]  id  char
        =============
        [0]   1  a
        [1]   1  b
        [2]   1  c
        [3]   2  a
        [4]   2  b
        [5]   2  c
        [6]   3  a
        [7]   3  b
        [8]   3  c

    Note that if the right frame has a column with the same column name as the current frame, the resulting frame
    will include a "_R" suffix in the column name from the right frame.  For example, if we cross join the frame with
    itself, it will result in a frame that has two columns: 'id' and 'id_R'.

        >>> self_cross_join = frame.join_cross(frame)

    <hide>
        >>> self_cross_join.sort(["id","id_R"])
    </hide>

        >>> self_cross_join.inspect()
        [#]  id  id_R
        =============
        [0]   1     1
        [1]   1     2
        [2]   1     3
        [3]   2     1
        [4]   2     2
        [5]   2     3
        [6]   3     1
        [7]   3     2
        [8]   3     3


    """

    from sparktk.frame.frame import Frame

    require_type(Frame, right, "right")

    return Frame(self._tc, self._scala.joinCross(right._scala))
