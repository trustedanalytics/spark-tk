
def download(self, n=100, offset = 0, columns=None):
    """
    Download frame data from the server into client workspace as a pandas dataframe.

    Similar to the 'take' function, but puts the data into a pandas dataframe.

    Parameters
    ----------

    :param n: (Optional(int)) The number of rows to download to the client from the frame (warning: do not overwhelm]
              this client by downloading too much).  Defaults to 100.
    :param offset: (Optional(int)) The number of rows to skip before copying.  Defaults to 0.
    :param columns: (Optional(List[str])) Column filter.  The list of names to be included.  Default is all columns.
    :return: (pandas.DataFrame) A new pandas dataframe object containing the downloaded frame data.

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

    The frame download() method is used to get a pandas DataFrame that contains the data from the spark-tk frame.  Note
    that since no parameters are provided when download() is called, the default values are used for the number of rows
    downloaded, the row offset, and the columns that were downloaded.

        >>> pandas_frame = frame.download()
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
    from sparktkpandas import sparktk_dtype_to_pandas_str
    result = self.take(n, offset, columns)
    headers, data_types = zip(*result.schema)
    pandas_df = pandas.DataFrame(result.data, columns=headers)
    for i, dtype in enumerate(data_types):
        dtype_str = sparktk_dtype_to_pandas_str(dtype)
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
