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


def to_pandas(self, n=None, offset=0, columns=None):
    """
    Brings data into a local pandas dataframe.

    Similar to the 'take' function, but puts the data into a pandas dataframe.

    Parameters
    ----------

    :param n: (Optional(int)) The number of rows to get from the frame (warning: do not overwhelm the python session
                    by taking too much)
    :param offset: (Optional(int)) The number of rows to skip before copying.  Defaults to 0.
    :param columns: (Optional(List[str])) Column filter.  The list of names to be included.  Default is all columns.
    :return: (pandas.DataFrame) A new pandas dataframe object containing the taken frame data.

    Examples
    --------

        <hide>
        >>> data = [["Fred", "555-1234"],["Susan", "555-0202"],["Thurston","555-4510"],["Judy","555-2183"]]
        >>> column_names = ["name", "phone"]
        >>> frame = tc.frame.create(data, column_names)
        </hide>

    Consider the following spark-tk frame, where we have columns for name and phone number:

        >>> frame.inspect()
        [#]  name      phone
        =======================
        [0]  Fred      555-1234
        [1]  Susan     555-0202
        [2]  Thurston  555-4510
        [3]  Judy      555-2183

        >>> frame.schema
        [('name', <type 'str'>), ('phone', <type 'str'>)]

    The frame to_pandas() method is used to get a pandas DataFrame that contains the data from the spark-tk frame.  Note
    that since no parameters are provided when to_pandas() is called, the default values are used for the number of
    rows, the row offset, and the columns.

        >>> pandas_frame = frame.to_pandas()
        >>> pandas_frame
               name     phone
        0      Fred  555-1234
        1     Susan  555-0202
        2  Thurston  555-4510
        3      Judy  555-2183

    """
    try:
        import pandas
    except:
        raise RuntimeError("pandas module not found, unable to download.  Install pandas or try the take command.")
    from sparktk.frame.ops.take import take_rich

    result = take_rich(self, n, offset, columns)
    headers, data_types = zip(*result.schema)
    frame_data = result.data

    from sparktk import dtypes
    import datetime

    date_time_columns = [i for i, x in enumerate(self.schema) if x[1] in (dtypes.datetime, datetime.datetime)]
    has_date_time = len(date_time_columns) > 0

    # translate our datetime long to datetime, so that it gets into the pandas df as a datetime column
    def long_to_date_time(row):
        for i in date_time_columns:
            if isinstance(row[i], long):
                row[i] = datetime.datetime.fromtimestamp(row[i]//1000).replace(microsecond=row[i]%1000*1000)
        return row

    if (has_date_time):
        frame_data = map(long_to_date_time, frame_data)

    # create pandas df
    pandas_df = pandas.DataFrame(frame_data, columns=headers)

    for i, dtype in enumerate(data_types):
        dtype_str = _sparktk_dtype_to_pandas_str(dtype)
        try:
            pandas_df[[headers[i]]] = pandas_df[[headers[i]]].astype(dtype_str)
        except (TypeError, ValueError):
            if dtype_str.startswith("int"):
                # DataFrame does not handle missing values in int columns. If we get this error, use the 'object' datatype instead.
                print "WARNING - Encountered problem casting column %s to %s, possibly due to missing values (i.e. presence of None).  Continued by casting column %s as 'object'" % (headers[i], dtype_str, headers[i])
                pandas_df[[headers[i]]] = pandas_df[[headers[i]]].astype("object")
            else:
                raise
    return pandas_df


def _sparktk_dtype_to_pandas_str(dtype):
    """maps spark-tk schema types to types understood by pandas, returns string"""
    from sparktk import dtypes
    if dtype ==dtypes.datetime:
        return "datetime64[ns]"
    elif dtypes.dtypes.is_primitive_type(dtype):
        return dtypes.dtypes.to_string(dtype)
    return "object"
