from setup import tc, rm, get_sandbox_path
from sparktk import dtypes

def test_import_csv(tc):
    path = "../datasets/importcsvtest.csv"
    # Test with inferred schema
    f = tc.frame.import_csv(path, header=True, inferschema=True)
    assert(f.row_count == 10)
    assert(len(f.schema) == 4)
    assert(f.schema == [("string_column", str),
                        ("integer_column", int),
                        ("float_column", float),
                        ("datetime_column", dtypes.datetime)])
    assert(f._is_python)
    # Make sure that we can go to scala (let's do that by binning the integer column)]
    f.bin_column("integer_column", [0,25,50,75,100])
    assert(len(f.schema) == 5)
    assert(f._is_scala)


def test_import_csv_with_custom_schema(tc):
    path = "../datasets/cities.csv"
    try:
        # Test with bad schema (incorrect number of columns)
        f = tc.frame.import_csv(path, "|", header=True, inferschema=False, schema=[("a", int),("b", str)])
        f.take(f.row_count)
        raise RuntimeError("Expected SparkException from import_csv due to incorrect number of columns in custom schema.")
    except:
        pass

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
    assert(len(f.schema) == 4)
    assert(f.schema == [('C0', str), ('C1', int), ('C2', float), ('C3', dtypes.datetime)])


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

def test_import_with_unsupported_type(tc):
    path = "../datasets/unsupported_types.csv"

    try:
        # Try creating a frame from a csv using inferschema.  This csv has a boolean type, which we don't support,
        # so this should fail.
        tc.frame.import_csv(path, ",", inferschema=True)
        raise RuntimeError("Expected TypeError for unsupported type found when inferring schema (boolean).")
    except TypeError:
        pass

    schema = [("id", int), ("name", str), ("bool", bool), ("day", str)]
    try:
        # Instead of inferring the schema, specify the schema that uses a boolean column.  This should still fail
        tc.frame.import_csv(path, ",", inferschema=False, schema=schema)
        raise RuntimeError("Expected TypeError for unsupported type in the schema (bool).")
    except TypeError:
        pass

    schema = [("id", int), ("name", str), ("bool", str), ("day", str)]

    # Specify the boolean column as a string instead.  This should pass
    frame = tc.frame.import_csv(path, ",", inferschema=False, schema=schema)
    assert(frame.row_count == 5)
    assert(frame.schema == schema)