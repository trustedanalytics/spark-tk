from sparktk.lazyloader import implicit
from sparktk.tkcontext import TkContext

def import_csv(path, delimiter=",", header=False, inferschema=True, schema=None, tc=implicit):
    """
    Creates a frame with data from a csv file.

    Parameters
    ----------

    :param path: (str) Full path to the csv file
    :param delimiter: (Optional[str]) A string which indicates the separation of data fields.  This is usually a
                      single character and could be a non-visible character, such as a tab. The default delimiter
                      is a comma (,).
    :param header: (Optional[bool]) Boolean value indicating if the first line of the file will be used to name columns,
                   and not be included in the data.  The default value is false.
    :param inferschema:(Optional[bool]) Boolean value indicating if the column types will be automatically inferred.
                       It requires one extra pass over the data and is false by default.
    :param: schema: (Optional[List[tuple(str, type)]]) Optionally specify the schema for the dataset.  Number of
                    columns specified in the schema must match the number of columns in the csv file provided.
    :return: (Frame) Frame that contains the data from the csv file

    Examples
    --------
    Load a frame from a csv file by specifying the path to the file, delimiter, and options that specify that
    there is a header and to infer the schema based on the data.

        >>> file_path = "../integration-tests/datasets/cities.csv"

        >>> frame = tc.load_frame_from_csv(file_path, "|", header=True, inferschema=True)
        -etc-

        >>> frame.inspect()
        [#]  rank  city         population_2013  population_2010  change  county
        ============================================================================
        [0]     1  Portland              609456           583776  4.40%   Multnomah
        [1]     2  Salem                 160614           154637  3.87%   Marion
        [2]     3  Eugene                159190           156185  1.92%   Lane
        [3]     4  Gresham               109397           105594  3.60%   Multnomah
        [4]     5  Hillsboro              97368            91611  6.28%   Washington
        [5]     6  Beaverton              93542            89803  4.16%   Washington
        [6]    15  Grants Pass            35076            34533  1.57%   Josephine
        [7]    16  Oregon City            34622            31859  8.67%   Clackamas
        [8]    17  McMinnville            33131            32187  2.93%   Yamhill
        [9]    18  Redmond                27427            26215  4.62%   Deschutes

        >>> frame.schema
        [('rank', int),
         ('city', str),
         ('population_2013', int),
         ('population_2010', int),
         ('change', str),
         ('county', str)]
    """

    from pyspark.sql import SQLContext

    if schema is not None:
        inferschema = False   # if a custom schema is provided, don't waste time inferring the schema during load
    if not isinstance(header, bool):
        raise ValueError("header parameter must be a boolean, but is {0}.".format(type(header)))
    if not isinstance(inferschema, bool):
        raise ValueError("inferschema parameter must be a boolean, but is {0}.".format(type(inferschema)))
    if tc is implicit:
        implicit.error('tc')
    if not isinstance(tc, TkContext):
        raise ValueError("tc must be type TkContext, received %s" % type(tc))

    header_str = str(header).lower()
    inferschema_str = str(inferschema).lower()
    sqlContext = SQLContext(tc.sc)
    df = sqlContext.read.format("com.databricks.spark.csv").options(delimiter=delimiter,
                                                                    header=header_str,
                                                                    inferschema=inferschema_str).load(path)
    df_schema = []

    if schema is None:
        for column in df.schema.fields:
            datatype = str
            import sparktk.dtypes as dtypes
            try:
                datatype = dtypes.dtypes.get_primitive_type_from_pyspark_type(type(column.dataType))
            except ValueError:
                print "Warning: No mapping for type: {0}. Column '{1}' will default to use strings.".format(str(column.dataType), column.name)
            df_schema.append((column.name, datatype))
    else:
        df_column_count = len(df.schema.fields)
        custom_column_count = len(schema)
        if (df_column_count != custom_column_count):
            raise ValueError("Bad schema value.  The number of columns in the custom schema ({0}) must match the"
                             "number of columns in the csv file data ({1}).".format(custom_column_count, df_column_count))
        df_schema = schema
    from sparktk.frame.frame import Frame  # circular dependency, so import late
    return Frame(tc, df.rdd, df_schema)
