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

from sparktk.tkcontext import TkContext
from sparktk import dtypes
import datetime
import logging
logger = logging.getLogger('sparktk')

def import_pandas(pandas_frame, schema=None, row_index=True, validate_schema=False, tc=TkContext.implicit):
    """
    Imports data from the specified pandas data frame.

    Parameters
    ----------

    :param pandas_frame: (pandas.DataFrame)  pandas dataframe object
    :param schema: (Optional(list[tuples(string, type)])) Schema description of the fields for a given line.  It is a
                   list of tuples which describe each field, (field name, field type), where the field name is a
                   string, and file is a supported type.  If no schema is provided, the schema will be inferred based
                   on the column names and types from the pandas_frame.
    :param row_index: (Optional(bool)) Indicates if the row_index is present in the pandas dataframe and needs to be
                      ignored when looking at the data values. Default value is True.
    :param validate_schema: (Optional(bool)) If true, validates the data against the schema and attempts to cast the
                            data to the specified type, if it does not match the schema.  Defaults to False.
    :return: (Frame) spark-tk frame that contains data from the pandas_frame

    Examples
    --------

    Create a pandas data frame:

        >>> import pandas
        >>> ratings_data = [[0, "invalid"], [1, "Very Poor"], [2, "Poor"], [3, "Average"], [4, "Good"], [5, "Very Good"]]
        >>> df = pandas.DataFrame(ratings_data, columns=['rating_id', 'rating_text'])

        >>> df
           rating_id rating_text
        0          0     invalid
        1          1   Very Poor
        2          2        Poor
        3          3     Average
        4          4        Good
        5          5   Very Good

        >>> df.columns.tolist()
        ['rating_id', 'rating_text']

        >>> df.dtypes
        rating_id       int64
        rating_text    object
        dtype: object

    When using import_pandas by just passing the pandas data frame, it will use the column names and types from the
    pandas data frame to generate the schema.

        >>> frame = tc.frame.import_pandas(df)

        >>> frame.inspect()
        [#]  rating_id  rating_text
        ===========================
        [0]          0  invalid
        [1]          1  Very Poor
        [2]          2  Poor
        [3]          3  Average
        [4]          4  Good
        [5]          5  Very Good

        >>> frame.schema
        [('rating_id', <type 'long'>), ('rating_text', <type 'str'>)]

    Alternatively, you can specify a schema when importing the pandas data frame.  There is also the option to validate
    the data against the schema.  If this option is enabled, we will attempt to cast the data to the column's data type,
    if it does not match the schema.

    For example, here we will specify a schema where the rating_id column will instead be called 'rating_float' and it's
    data type will be a float.  We will also enable the validate_schema option so that the rating_id value will get
    casted to a float:
        >>> schema = [("rating_float", float), ("rating_str", unicode)]
        >>> frame = tc.frame.import_pandas(df, schema, validate_schema=True)

        >>> frame.inspect()
        [#]  rating_float  rating_str
        =============================
        [0]           0.0  invalid
        [1]           1.0  Very Poor
        [2]           2.0  Poor
        [3]           3.0  Average
        [4]           4.0  Good
        [5]           5.0  Very Good

        >>> frame.schema
        [('rating_float', <type 'float'>), ('rating_str', <type 'unicode'>)]

    """
    try:
        import pandas
    except:
        raise RuntimeError("pandas module not found, unable to download.  Install pandas or try the take command.")

    if not isinstance(pandas_frame, pandas.DataFrame):
        raise TypeError("data_frame must be a pandas DataFrame.")
    TkContext.validate(tc)
    if schema is not None:
        schema = _validate(schema)
    else:
        schema = _get_schema_from_df(pandas_frame)

    if not row_index:
        pandas_frame = pandas_frame.reset_index()

    pandas_frame = pandas_frame.dropna(thresh=len(pandas_frame.columns))
    field_names = [x[0] for x in schema]
    if len(pandas_frame.columns) != len(field_names):
        raise ValueError("Number of columns in Pandasframe {0} does not match the number of columns in the"
                         " schema provided {1}.".format(len(pandas_frame.columns), len(field_names)))

    date_time_columns = [i for i, x in enumerate(pandas_frame.dtypes) if x == "datetime64[ns]"]
    has_date_time = len(date_time_columns) > 0

    # pandas gives us the date/time in nm or as a Timestamp, and spark-tk expects it as ms, so we need to do the conversion
    def pandas_datetime_to_ms(row):
        for i in date_time_columns:
            if isinstance(row[i], long):
                row[i] = row[i] / 1000000
            elif isinstance(row[i], pandas.tslib.Timestamp) or isinstance(row[i], datetime):
                dt = row[i]
                # get number of seconds since epoch (%s) and multiply by 1000 for ms then get the
                # microseconds to get the ms precision.
                row[i] = long((long(dt.strftime("%s")) * 1000) + (dt.microsecond // 1000))
        return row

    pandas_rows = pandas_frame[0:len(pandas_frame.index)].values.tolist()

    # if the dataframe has date/time columns, map them to ms
    if (has_date_time):
        pandas_rows = map(pandas_datetime_to_ms, pandas_rows)

    # create frame with the pandas_rows
    frame = tc.frame.create(pandas_rows, schema)

    if validate_schema:
        frame = tc.frame.create(frame.rdd, schema, validate_schema)

    return frame

# map pandas data type strings to spark-tk schema types
_pandas_type_to_type_table = {
    "datetime64[ns]": dtypes.datetime,
    "object": str,
    "int64": long,
    "int32": int,
    "float32": float,
    "uint8": int,
}

def _get_schema_from_df(pandas_frame):
    """
    Creates a spark-tk schema list from the specified pandas data frame.

    :param pandas_frame: (pandas.DataFrame) pandas data frame to get column information
    :return: (list[tuple(str, type)]) schema
    """
    try:
        import pandas
    except:
        raise RuntimeError("pandas module not found, unable to download.  Install pandas or try the take command.")
    if not isinstance(pandas_frame, pandas.DataFrame):
        raise TypeError("pandas_frame must be a pandas DataFrame.")

    column_names = pandas_frame.columns.tolist()

    schema = []

    for i, dtype in enumerate(pandas_frame.dtypes):
        dtype_str = str(dtype)
        if _pandas_type_to_type_table.has_key(dtype_str):
            schema.append((column_names[i], _pandas_type_to_type_table[dtype_str]))
        else:
            logger.warn("Unsupported column type {0} for column {1}. Schema will use a str.").format(dtype_str, column_names[i])
            schema.append(column_names[i], str)
    return schema

def _validate(schema):
    """
    Validates the specified schema

    :param schema: (list[tuple(str, type)]) schema to validate
    :return: (list[tuple(str, type)]) validated schema
    """
    if not isinstance(schema, list) or len(schema) == 0:
        raise TypeError("schema must be a non-empty list of tuples")
    validated_schema = []
    for field in schema:
        if not isinstance(field, tuple):
            raise TypeError("schema must be a list of tuples (column name string, type).")
        if len(field) != 2:
            raise TypeError("schema tuples are expected to have 2 items, but found " + len(field))
        name = field[0]
        if not isinstance(name, basestring):
            raise ValueError("First item in schema tuple must be a string")
        try:
            data_type = dtypes.dtypes.get_from_type(field[1])
        except ValueError:
            raise ValueError("Second item in schema tuple must be a supported type: " + str(dtypes.dtypes))
        else:
            validated_schema.append((name, data_type))
    return validated_schema
