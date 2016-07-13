from setup import tc

data = [["Bob", 30, 8], ["Jim", 45, 9.5], ["Sue", 25, 7], ["George", 15, 6], ["Jennifer", 18, 8.5]]

def test_rename_single_column(tc):
    schema = [("name", str), ("age", int), ("shoe_size", float)]
    new_column_name = {"name": "nick"}
    frame = tc.frame.create(data, schema)
    frame.rename_columns(new_column_name)
    assert(frame.schema == [("nick", str), ("age", int), ("shoe_size", float)])

def test_rename_single_column_to_existing_name(tc):
    schema = [("name", str), ("age", int), ("shoe_size", float)]
    new_column_name = {"name": "shoe_size"}
    frame = tc.frame.create(data, schema)
    try:
        frame.rename_columns(new_column_name)
        raise RuntimeError("Expected ValueError when passing column name which exists")
    except ValueError:
        pass

def test_rename_multiple_columns(tc):
    schema = [("name", str), ("age", int), ("shoe_size", float)]
    new_column_name = {"name": "Name", "age": "Age"}
    frame = tc.frame.create(data, schema)
    frame.rename_columns(new_column_name)
    assert(frame.schema == [("Name", str), ("Age", int), ("shoe_size", float)])

def test_rename_multiple_columns_when_first_exists(tc):
    schema = [("name", str), ("age", int), ("shoe_size", float)]
    frame = tc.frame.create(data, schema)
    new_column_name = {"name": "shoe_size", "age": "Age"}
    try:
        frame.rename_columns(new_column_name)
        raise RuntimeError("Expected ValueError when passing multiple columns names and one of them exists")
    except ValueError:
        pass

def test_rename_multiple_columns_when_second_exists(tc):
    schema = [("name", str), ("age", int), ("shoe_size", float)]
    new_column_name = {"name": "Name", "age": "shoe_size"}
    frame = tc.frame.create(data, schema)
    try:
        frame.rename_columns(new_column_name)
        raise RuntimeError("Expected ValueError when passing multiple columns names and one of them exists")
    except ValueError:
        pass

def test_rename_multiple_columns_when_first_is_new_and_second_is_old(tc):
    schema = [("name", str), ("age", int), ("shoe_size", float)]
    new_column_name = {"name": "Name", "age": "name"}
    frame = tc.frame.create(data, schema)
    frame.rename_columns(new_column_name)
    assert(frame.schema == [("Name", str), ("name", int), ("shoe_size", float)])

def test_rename_multiple_columns_changing(tc):
    schema = [("name", str), ("age", int), ("shoe_size", float)]
    new_column_name = {"name": "age", "age": "name"}
    frame = tc.frame.create(data, schema)
    frame.rename_columns(new_column_name)
    assert(frame.schema == [("age", str), ("name", int), ("shoe_size", float)])


def test_rename_multiple_not_unique_names(tc):
    schema = [("name", str), ("age", int), ("shoe_size", float)]
    new_column_name = {"name": "not_unique", "age": "not_unique"}
    frame = tc.frame.create(data, schema)
    try:
        frame.rename_columns(new_column_name)
        raise RuntimeError("Expected ValueError when passing duplicated multiple columns names")
    except ValueError:
        pass

def test_rename_multiple_unique_and_not_unique_names(tc):
    schema = [("name", str), ("age", int), ("shoe_size", float)]
    new_column_name = {"name": "unique", "age": "not_unique", "shoe_size": "not_unique"}
    frame = tc.frame.create(data, schema)
    try:
        frame.rename_columns(new_column_name)
        raise RuntimeError("Expected ValueError when passing duplicated multiple columns names and one of them is unique")
    except ValueError:
        pass