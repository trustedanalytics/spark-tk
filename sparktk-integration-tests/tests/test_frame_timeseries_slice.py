from setup import tc, rm, get_sandbox_path
from sparktk import dtypes

schema= [("key", str), ("series", dtypes.vector(6))]
data = [["A", [62,55,60,61,60,59]],["B", [60,58,61,62,60,61]],["C", [69,68,68,70,71,69]]]
datetimeindex = ["2016-01-01T12:00:00.000Z","2016-01-02T12:00:00.000Z","2016-01-03T12:00:00.000Z","2016-01-04T12:00:00.000Z","2016-01-05T12:00:00.000Z","2016-01-06T12:00:00.000Z"]

def test_date_without_time(tc):
    """
    Tests using start/end dates (no times) and verifies that we get back a frame with the expected number of rows
    """
    ts_frame = tc.frame.create(data, schema)
    start = "2016-01-02"
    end = "2016-01-04"
    f = ts_frame.timeseries_slice(datetimeindex, start, end)
    assert(f.count() == 3)

def test_invalid_string_start(tc):
    """
    Tests calling time series slice with an invalid start date string
    """
    ts_frame = tc.frame.create(data, schema)
    try:
        start = "abc"
        end = "2016-01-04T12:00:00.000Z"
        ts_frame.timeseries_slice(datetimeindex, start, end)
        raise RuntimeError("Expected exception from invalid start date: " + start)
    except:
        pass

def test_invalid_string_end(tc):
    """
    Tests calling time series slice with an invalid end date string
    """
    ts_frame = tc.frame.create(data, schema)
    try:
        start = "2016-01-02T12:00:00.000Z"
        end = "abc"
        ts_frame.timeseries_slice(datetimeindex, start, end)
        raise RuntimeError("Expected exception from invalid end date: " + end)
    except:
        pass

def test_invalid_int_start(tc):
    """
    Tests calling time series slice with an invalid start date integer
    """
    ts_frame = tc.frame.create(data, schema)
    try:
        start = 7
        end = "2016-01-04T12:00:00.000Z"
        ts_frame.timeseries_slice(datetimeindex, start, end)
        raise RuntimeError("Expected exception from invalid start date: " + str(start))
    except TypeError:
        pass
    except:
        raise RuntimeError("Expected a TypeError from an invalid (non-string) start date.")

def test_invalid_int_end(tc):
    """
    Tests calling time series slice with an invalid end date integer
    """
    ts_frame = tc.frame.create(data, schema)
    try:
        start = "2016-01-02T12:00:00.000Z"
        end = 7
        ts_frame.timeseries_slice(datetimeindex, start, end)
        raise RuntimeError("Expected exception from invalid end date: " + str(end))
    except TypeError:
        pass
    except:
        raise RuntimeError("Expected a TypeError from an invalid (non-string) end date.")