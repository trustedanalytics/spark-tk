from setup import tc

data = [["Bob", 30, 8], ["Jim", 45, 9.5], ["Sue", 25, 7], ["George", 15, 6], ["Jennifer", 18, 8.5]]

def test_rename_single_column(tc):
    schema = [("name", str), ("age", int), ("shoe_size", float)]
    new_column_names = {"name": "nick"}
    frame = tc.frame.create(data, schema)
    assert (frame._is_python)
    frame.rename_columns(new_column_names)
    assert(frame.column_names == ["nick", "age", "shoe_size"])
    frame = tc.frame.create(data, schema)
    frame._scala
    assert (frame._is_scala)
    frame.rename_columns(new_column_names)
    assert(frame.column_names == ["nick", "age", "shoe_size"])

def test_rename_single_column_to_existing_name(tc):
    schema = [("name", str), ("age", int), ("shoe_size", float)]
    new_column_names = {"name": "shoe_size"}
    frame = tc.frame.create(data, schema)
    assert (frame._is_python)
    try:
        frame.rename_columns(new_column_names)
        raise RuntimeError("Expected ValueError when passing column name which exists")
    except Exception as e:
        assert(frame.column_names == ["name", "age", "shoe_size"])
        assert("existing names" in str(e))
    frame = tc.frame.create(data, schema)
    frame._scala
    assert (frame._is_scala)
    try:
        frame.rename_columns(new_column_names)
        raise RuntimeError("Expected ValueError when passing column name which exists")
    except Exception as e:
        assert(frame.column_names == ["name", "age", "shoe_size"])
        assert("existing names" in str(e))

def test_rename_multiple_columns(tc):
    schema = [("name", str), ("age", int), ("shoe_size", float)]
    new_column_names = {"name": "Name", "age": "Age"}
    frame = tc.frame.create(data, schema)
    assert (frame._is_python)
    frame.rename_columns(new_column_names)
    assert(frame.column_names == ["Name", "Age", "shoe_size"])
    frame = tc.frame.create(data, schema)
    frame._scala
    assert (frame._is_scala)
    frame.rename_columns(new_column_names)
    assert(frame.column_names == ["Name", "Age", "shoe_size"])

def test_rename_multiple_columns_when_first_exists(tc):
    schema = [("name", str), ("age", int), ("shoe_size", float)]
    new_column_names = {"name": "shoe_size", "age": "Age"}
    frame = tc.frame.create(data, schema)
    assert (frame._is_python)
    try:
        frame.rename_columns(new_column_names)
        raise RuntimeError("Expected ValueError when passing multiple columns names and first of them exists")
    except Exception as e:
        assert(frame.column_names == ["name", "age", "shoe_size"])
        assert("existing names" in str(e))
    frame = tc.frame.create(data, schema)
    frame._scala
    assert (frame._is_scala)
    try:
        frame.rename_columns(new_column_names)
        raise RuntimeError("Expected ValueError when passing multiple columns names and first of them exists")
    except Exception as e:
        assert(frame.column_names == ["name", "age", "shoe_size"])
        assert("existing names" in str(e))

def test_rename_multiple_columns_when_second_exists(tc):
    schema = [("name", str), ("age", int), ("shoe_size", float)]
    new_column_names = {"name": "Name", "age": "shoe_size"}
    frame = tc.frame.create(data, schema)
    assert (frame._is_python)
    try:
        frame.rename_columns(new_column_names)
        raise RuntimeError("Expected ValueError when passing multiple columns names and second exists")
    except Exception as e:
        assert(frame.column_names == ["name", "age", "shoe_size"])
        assert("existing names" in str(e))
    frame = tc.frame.create(data, schema)
    frame._scala
    assert (frame._is_scala)
    try:
        frame.rename_columns(new_column_names)
        raise RuntimeError("Expected ValueError when passing multiple columns names and second exists")
    except Exception as e:
        assert(frame.column_names == ["name", "age", "shoe_size"])
        assert("existing names" in str(e))

def test_rename_multiple_columns_when_first_is_new_and_second_is_old(tc):
    schema = [("name", str), ("age", int), ("shoe_size", float)]
    new_column_names = {"name": "Name", "age": "name"}
    frame = tc.frame.create(data, schema)
    assert (frame._is_python)
    frame.rename_columns(new_column_names)
    assert(frame.column_names == ["Name", "name", "shoe_size"])
    frame = tc.frame.create(data, schema)
    frame._scala
    assert (frame._is_scala)
    frame.rename_columns(new_column_names)
    assert(frame.column_names == ["Name", "name", "shoe_size"])

def test_rename_multiple_columns_changing(tc):
    schema = [("name", str), ("age", int), ("shoe_size", float)]
    new_column_names = {"name": "age", "age": "name"}
    frame = tc.frame.create(data, schema)
    assert (frame._is_python)
    frame.rename_columns(new_column_names)
    assert(frame.column_names == ["age", "name", "shoe_size"])
    frame = tc.frame.create(data, schema)
    frame._scala
    assert (frame._is_scala)
    frame.rename_columns(new_column_names)
    assert(frame.column_names == ["age", "name", "shoe_size"])

def test_rename_multiple_not_unique_names(tc):
    schema = [("name", str), ("age", int), ("shoe_size", float)]
    new_column_names = {"name": "not_unique", "age": "not_unique"}
    frame = tc.frame.create(data, schema)
    assert (frame._is_python)
    try:
        frame.rename_columns(new_column_names)
        raise RuntimeError("Expected ValueError when passing multiple duplicated columns names")
    except Exception as e:
        assert frame.column_names == ["name", "age", "shoe_size"]
        assert("Invalid new column names are not unique" in str(e))
    frame = tc.frame.create(data, schema)
    frame._scala
    assert (frame._is_scala)
    try:
        frame.rename_columns(new_column_names)
        raise RuntimeError("Expected ValueError when passing multiple duplicated columns names")
    except Exception as e:
        assert frame.column_names == ["name", "age", "shoe_size"]
        assert("Invalid new column names are not unique" in str(e))

def test_rename_multiple_unique_and_not_unique_names(tc):
    schema = [("name", str), ("age", int), ("shoe_size", float)]
    new_column_names = {"name": "unique", "age": "not_unique", "shoe_size": "not_unique"}
    frame = tc.frame.create(data, schema)
    assert (frame._is_python)
    try:
        frame.rename_columns(new_column_names)
        raise RuntimeError("Expected ValueError when passing multiple duplicated columns names and one of them is unique")
    except Exception as e:
        assert frame.column_names == ["name", "age", "shoe_size"]
        assert("Invalid new column names are not unique" in str(e))
    frame = tc.frame.create(data, schema)
    frame._scala
    assert (frame._is_scala)
    try:
        frame.rename_columns(new_column_names)
        raise RuntimeError("Expected ValueError when passing multiple duplicated columns names and one of them is unique")
    except Exception as e:
        assert frame.column_names == ["name", "age", "shoe_size"]
        assert("Invalid new column names are not unique" in str(e))