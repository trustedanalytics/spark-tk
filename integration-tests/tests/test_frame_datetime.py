from setup import tc, rm, get_sandbox_path
from sparktk import dtypes

def test_frame_datetime(tc):
    """
    Create a frame with datetime values and check the values from frame.
    Take the value to scala and then again check the values.
    """
    data = [[1, "Bob", "1950-05-12T03:25:21.123000Z"],
            [2, "Susan", "1979-08-05T07:51:28.535000Z"],
            [3, "Jane", "1986-10-17T11:45:00.183000Z"]]
    frame = tc.frame.create(data, [("id", int), ("name", str), ("bday", dtypes.datetime)])
    assert(frame._is_python)
    assert(frame.row_count == 3)
    assert(frame.take(frame.row_count).data == data)

    # frame to scala
    frame._scala
    assert(frame._is_scala)
    frame_data = frame.take(frame.row_count).data
    for original, row in zip(data, frame_data):
        assert(len(original) == len(row) == 3)
        assert(original[0] == row[0])
        assert(original[1] == row[1])
        # After going to scala, the "bday" column uses the long type.  Convert it to a string to compare with the original data
        assert(original[2] == dtypes.ms_to_datetime_str(row[2]))

    # back to python
    frame._python
    assert(frame._is_python)
    frame_data = frame.take(frame.row_count).data
    for original, row in zip(data, frame_data):
        assert(len(original) == len(row) == 3)
        assert(original[0] == row[0])
        assert(original[1] == row[1])
        # After going to scala, the "bday" column uses the long type.  Convert it to a string to compare with the original data
        assert(original[2] == dtypes.ms_to_datetime_str(row[2]))