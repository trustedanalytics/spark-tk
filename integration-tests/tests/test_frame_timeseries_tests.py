from setup import tc, rm, get_sandbox_path

def test_adf_invalid_params(tc):
    """
    Test the Augmented Dickey-Fuller test with invalid parameters
    """
    data = [[12.88969427], [13.54964408], [13.8432745], [12.13843611], [12.81156092], [14.2499628], [15.12102595]]
    frame = tc.frame.create(data, schema=["data"])

    try:
        frame.augmented_dickey_fuller_test(10, 0)
        raise RuntimeError("Expected TypeError due to invalid ts_column type")
    except TypeError as e:
        assert("ts_column parameter should be a str" in e.message)

    try:
        frame.augmented_dickey_fuller_test("data", "invalid lag")
        raise RuntimeError("Expected TypeError due to invalid max_lag type")
    except TypeError as e:
        assert("max_lag parameter should be a int" in e.message)

    try:
        frame.augmented_dickey_fuller_test("data", 0, 123)
        raise RuntimeError("Expected TypeError due to invalid regression type")
    except TypeError as e:
        assert("regression parameter should be a str" in e.message)

def test_adf_column_types(tc):
    """
    Tests the Augmented Dickey-Fuller test with different column types
    """
    data = [[1, "a", 1.5], [2, "b", 18.5], [4, "c", 22.1], [5, "d", 19.0], [7, "e", 25.6], [8, "f", 36.75]]
    schema = [("int_column", int), ("str_column", str), ("float_column", float)]
    frame = tc.frame.create(data, schema)

    try:
        # string column should have an error
        frame.augmented_dickey_fuller_test("str_column", 0)
        raise RuntimeError("Expected error since the str_column is not numerical.")
    except Exception as e:
        assert("Column str_column was not numerical" in e.message)

    # Numerical columns should not have an error
    frame.augmented_dickey_fuller_test("int_column", 0)
    frame.augmented_dickey_fuller_test("float_column", 0)


def test_dwtest_invalid_params(tc):
    """
    Test the Durbin-Watson test with an invalid parameter
    """
    data = [[12.88969427], [13.54964408], [13.8432745], [12.13843611], [12.81156092], [14.2499628], [15.12102595]]
    frame = tc.frame.create(data)

    try:
        # Parameter should be a column name str
        frame.durbin_watson_test(1)
        raise RuntimeError("Expected TypeError due to invalid parameter type.")
    except TypeError as e:
        assert("residuals should be a str" in e.message)

def test_dwtest_column_types(tc):
    """
    Tests that the Durbin-Watson test only works with numerical columns
    """
    data = [[1, "a", 1.5], [2, "b", 18.5], [4, "c", 22.1], [5, "d", 19.0], [7, "e", 25.6], [8, "f", 36.75]]
    schema = [("int_column", int), ("str_column", str), ("float_column", float)]
    frame = tc.frame.create(data, schema)

    try:
        # calling durbin-watson with a string column should fail
        frame.durbin_watson_test("str_column")
        raise RuntimeError("Expected error since the column must be numerical")
    except Exception as e:
        assert("Column str_column was not numerical" in e.message)

    # int and float columns should not give any error
    frame.durbin_watson_test("int_column")
    frame.durbin_watson_test("float_column")

def test_bgt_invalid_params(tc):
    """
    Tests the Breusch-Godfrey test with invalid parameters
    """
    data = [[1, "a", 1.5], [2, "b", 18.5], [4, "c", 22.1], [5, "d", 19.0], [7, "e", 25.6], [8, "f", 36.75]]
    schema = [("int_column", int), ("str_column", str), ("float_column", float)]
    frame = tc.frame.create(data, schema)

    try:
        frame.breusch_godfrey_test(1, ["int_column", "float_column"], 1)
        raise RuntimeError("Expected an error since residuals parameter should be a string")
    except TypeError as e:
        assert("residuals parameter should be a str" in e.message)

    try:
        frame.breusch_godfrey_test("int_column", 10, 1)
        raise RuntimeError("Expected an error since the factors parameter should be a list of strings")
    except Exception as e:
        assert("factors parameter should be a list of strings" in e.message)

    try:
        frame.breusch_godfrey_test("int_column", ["bogus"], 1)
        raise RuntimeError("Expected an error since there is no column named 'bogus'")
    except Exception as e:
        assert("No column named bogus" in e.message)

    try:
        frame.breusch_godfrey_test("int_column", ["float_column"], "bogus")
        raise RuntimeError("Expected an error, since the max_lag parameter should be an integer.")
    except Exception as e:
        assert("max_lag parameter should be an integer" in e.message)

def test_bgt_invalid_column(tc):
    """
    Tests the Breusch-Godfrey test with non-numerical data, and expects an error
    """
    data = [[1, "a", 1.5], [2, "b", 18.5], [4, "c", 22.1], [5, "d", 19.0], [7, "e", 25.6], [8, "f", 36.75]]
    schema = [("int_column", int), ("str_column", str), ("float_column", float)]
    frame = tc.frame.create(data, schema)

    try:
        frame.breusch_godfrey_test("str_column", ["int_column", "float_column"], max_lag=1)
        raise RuntimeError("Expected error since the y column specified has strings")
    except Exception as e:
        assert("Column str_column was not numerical" in e.message)

    try:
        frame.breusch_godfrey_test("float_column", ["int_column", "str_column"], 1)
        raise RuntimeError("Expected error since one of the x columns specified has strings.", max_lag=1)
    except Exception as e:
        assert("Column str_column was not numerical" in e.message)

    # numerical data should not have an error
    frame.breusch_godfrey_test("float_column", ["int_column"], max_lag=1)




