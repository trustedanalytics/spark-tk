from setup import tc, rm, get_sandbox_path

def test_import_csv(tc):
    path = "../datasets/importcsvtest.csv"
    # Test with inferred schema
    f = tc.frame.import_csv(path, header=True, inferschema=True)
    assert(f.row_count == 10)
    assert(len(f.schema) == 5)
    assert(f.schema == [("string_column", str),
                        ("integer_column", int),
                        ("float_column", float),
                        ("bool_column", bool),
                        ("datetime_column", str)])
    assert(f._is_python)
    # Test a frame function that uses python (i.e. drop_columns)
    f.drop_columns("bool_column")
    assert(len(f.schema) == 4)
    assert(f._is_python)
    # Make sure that we can go to scala (let's do that by binning the integer column)]
    f.bin_column("integer_column", [0,25,50,75,100])
    assert(len(f.schema) == 5)
    assert(f._is_scala)


def test_import_csv_with_custom_schema(tc):
    path = "../datasets/cities.csv"
    try:
        # Test with bad schema (incorrect number of columns)
        tc.frame.import_csv(path, "|", header=True, inferschema=False, schema=[("a", int),("b", str)])
        raise RuntimeError("Expected ValueError from import_csv due to incorrect number of columns in custom schema.")
    except ValueError:
        pass
    except:
        raise RuntimeError("Expected ValueError from import_csv due to incorrect number of columns in custom schema.")

    # Test with good schema
    schema = [("a",int),("b",str),("c",int),("d",int),("e",str),("f",str)]
    f = tc.frame.import_csv(path, "|", header=True, inferschema=False, schema=schema)
    assert(f.row_count == 20)
    assert(f.schema == schema)


def test_import_csv_with_no_header(tc):
    path = "../datasets/noheader.csv"
    # Test with no header and inferred schema
    f = tc.frame.import_csv(path, header=False, inferschema=True)
    assert(f.row_count == 10)
    assert(len(f.schema) == 5)
    assert(f.schema == [('C0', str), ('C1', int), ('C2', float), ('C3', bool), ('C4', str)])


def test_import_csv_with_invalid_header(tc):
    path = "../datasets/cities.csv"
    try:
        # Test with non-boolean header value
        tc.frame.import_csv(path, "|", header=5)
        raise RuntimeError("Expected ValueError from import_csv due to invalid (int) header parameter data type.")
    except ValueError:
        pass
    except:
        raise RuntimeError("Expected ValueError from import_csv due to invalid (int) header parameter data type.")

    try:
        # Test with non-boolean header value
        tc.frame.import_csv(path, "|", header="true")
        raise RuntimeError("Expected ValueError from import_csv due to invalid (string) header parameter data type.")
    except ValueError:
        pass
    except:
        raise RuntimeError("Expected ValueError from import_csv due to invalid (string) header parameter data type.")


def test_import_csv_with_invalid_inferschema(tc):
    path = "../datasets/cities.csv"
    try:
        # Test with non-boolean inferschema value
        tc.frame.import_csv(path, "|", inferschema=5)
        raise RuntimeError( "Expected ValueError from import_csv due to invalid (int) inferschema parameter data type.")
    except ValueError:
        pass
    except:
        raise RuntimeError( "Expected ValueError from import_csv due to invalid (int) inferschema parameter data type.")

    try:
        # Test with non-boolean inferschema value
        tc.frame.import_csv(path, "|", inferschema="true")
        raise RuntimeError("Expected ValueError from import_csv due to invalid (string) inferschema parameter data type.")
    except ValueError:
        pass
    except:
        raise RuntimeError("Expected ValueError from import_csv due to invalid (string) inferschema parameter data type.")