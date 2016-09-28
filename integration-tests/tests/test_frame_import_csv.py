# coding=utf-8

from setup import tc, rm, get_sandbox_path
from sparktk import dtypes

def test_import_csv(tc):
    path = "../datasets/importcsvtest.csv"
    # Test with inferred schema
    f = tc.frame.import_csv(path, header=True, infer_schema=True)
    assert(f.count() == 10)
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
        f = tc.frame.import_csv(path, "|", header=True, infer_schema=False, schema=[("a", int),("b", str)])
        f.take(f.count())
        raise RuntimeError("Expected SparkException from import_csv due to incorrect number of columns in custom schema.")
    except:
        pass

    # Test with good schema
    schema = [("a",int),("b",str),("c",int),("d",int),("e",str),("f",str)]
    f = tc.frame.import_csv(path, "|", header=True, infer_schema=False, schema=schema)
    assert(f.count() == 20)
    assert(f.schema == schema)

def test_import_csv_with_custom_schema_parse_error(tc):
    # Test with good schema, but bad values in file --bad values should render as None
    path = "../datasets/parse_error.csv"
    f = tc.frame.import_csv(path, schema=[("a", str),("b", int), ("c", float)], header=True)
    rows = f.take(f.count())
    assert(len(rows) == 4)
    assert(rows[2] == ["blue",100, None])         # bad float
    assert(rows[3] == ["purple",None, 3.33333])   # bad integer


def test_import_csv_with_no_header(tc):
    path = "../datasets/noheader.csv"
    # Test with no header and inferred schema
    f = tc.frame.import_csv(path, header=False, infer_schema=True)
    assert(f.count() == 10)
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


def test_import_csv_with_invalid_infer_schema(tc):
    path = "../datasets/cities.csv"
    try:
        # Test with non-boolean infer_schema value
        tc.frame.import_csv(path, "|", infer_schema=5)
        raise RuntimeError( "Expected ValueError from import_csv due to invalid (int) infer_schema parameter data type.")
    except ValueError:
        pass
    except:
        raise RuntimeError( "Expected ValueError from import_csv due to invalid (int) infer_schema parameter data type.")

    try:
        # Test with non-boolean infer_schema value
        tc.frame.import_csv(path, "|", infer_schema="true")
        raise RuntimeError("Expected ValueError from import_csv due to invalid (string) infer_schema parameter data type.")
    except ValueError:
        pass
    except:
        raise RuntimeError("Expected ValueError from import_csv due to invalid (string) infer_schema parameter data type.")

def test_import_with_unsupported_type(tc):
    path = "../datasets/unsupported_types.csv"

    try:
        # Try creating a frame from a csv using infer_schema.  This csv has a boolean type, which we don't support,
        # so this should fail.
        tc.frame.import_csv(path, ",", infer_schema=True)
        raise RuntimeError("Expected TypeError for unsupported type found when inferring schema (boolean).")
    except TypeError:
        pass

    schema = [("id", int), ("name", str), ("bool", bool), ("day", str)]
    try:
        # Instead of inferring the schema, specify the schema that uses a boolean column.  This should still fail
        tc.frame.import_csv(path, ",", infer_schema=False, schema=schema)
        raise RuntimeError("Expected TypeError for unsupported type in the schema (bool).")
    except TypeError:
        pass

    schema = [("id", int), ("name", str), ("bool", str), ("day", str)]

    # Specify the boolean column as a string instead.  This should pass
    frame = tc.frame.import_csv(path, ",", infer_schema=False, schema=schema)
    assert(frame.count() == 5)
    assert(frame.schema == schema)

def test_frame_loading_multiple_files_with_wildcard(tc):
        frame = tc.frame.import_csv("../datasets/movie-part*.csv", header=True)
        assert(frame.schema == [('user', int),
                                ('vertex_type', str),
                                ('movie', int),
                                ('weight', int),
                                ('edge_type', str)])
        assert(frame.take(frame.count()) == [[1, 'L', -131, 0, 'tr'],
                                                    [-131, 'R', 1, 0, 'tr'],
                                                    [1, 'L', -300, 2, 'tr'],
                                                    [-300, 'R', 1, 2, 'tr'],
                                                    [1, 'L', -570, 4, 'tr'],
                                                    [-570, 'R', 1, 4, 'tr'],
                                                    [2, 'L', -778, 1, 'tr'],
                                                    [-778, 'R', 1, 1, 'tr'],
                                                    [1, 'L', -1209, 2, 'va'],
                                                    [-1209, 'R', 1, 2, 'va'],
                                                    [2, 'L', -1218, 3, 'tr'],
                                                    [-1218, 'R', 1, 3, 'tr'],
                                                    [1, 'L', -1232, 3, 'tr'],
                                                    [-1232, 'R', 1, 3, 'tr'],
                                                    [3, 'L', -1416, 2, 'tr'],
                                                    [-1416, 'R', 1, 2, 'tr'],
                                                    [3, 'L', -1648, 3, 'tr'],
                                                    [-1648, 'R', 1, 3, 'tr'],
                                                    [3, 'L', -2347, 3, 'tr'],
                                                    [-2347, 'R', 1, 3, 'tr']])
def test_import_csv_with_duplicate_coluns(tc):
    path = "../datasets/importcsvtest.csv"
    schema = [("string", str),
              ("numeric", int),
              ("numeric", float),
              ("datetime", dtypes.datetime)]
    try:
        # Try to create a frame from csv, using a schema that has duplicate column names
        tc.frame.import_csv(path, schema=schema, header=True, infer_schema=False)
    except Exception as e:
        assert("duplicate entry: 'numeric'" in str(e))

def test_import_csv_with_unicode_columns(tc):
    path = "../datasets/unicode.csv"
    schema = [("a", unicode),("b", unicode),("c",unicode)]
    frame = tc.frame.import_csv(path, schema=schema, header=False, infer_schema=False)
    expected_data = [[u'à',u'ë',u'ñ'],[u'ã',u'ê',u'ü']]
    assert(frame.take(frame.count()) == expected_data)
