from setup import tc, rm, get_sandbox_path
from sparktk import dtypes

def test_frame_to_pandas_to_frame(tc):
    """
    Tests going from a frame to a pandas df (to_pandas) and then back to a frame (import_pandas)
    """
    # Create a frame from a csv file for testing
    path = "../datasets/importcsvtest.csv"
    frame1 = tc.frame.import_csv(path, header=True, infer_schema=True)

    # bring to data frame and check the columns/types/row count
    df = frame1.to_pandas()
    assert(df.columns.tolist() == ['string_column', 'integer_column', 'float_column', 'datetime_column'])
    assert([str(d) for d in df.dtypes] == ['object', 'int32', 'float64', 'datetime64[ns]'])
    assert(frame1.count() == len(df))

    # import the data frame back to a frame
    frame2 = tc.frame.import_pandas(df, frame1.schema, validate_schema=True)

    # compare this frame to the original frame
    assert(len(frame1.schema) == len(frame2.schema))
    for col1, col2 in zip(frame1.schema, frame2.schema):
        assert(col1[0] == col2[0])
        assert(dtypes.dtypes.get_from_type(col1[1]) == dtypes.dtypes.get_from_type(col2[1]))
    assert(frame2.take(frame2.count()) == frame1.take(frame1.count()))


