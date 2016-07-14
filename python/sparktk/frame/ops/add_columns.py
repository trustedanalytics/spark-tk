from sparktk.frame.row import Row

from sparktk.frame.schema import is_mergeable

    # @api
    # @has_udf_arg
    # @arg('func', 'UDF', "User-Defined Function (|UDF|) which takes the values in the row and produces a value, or "
    #      "collection of values, for the new cell(s).")
    # @arg('schema', 'tuple | list of tuples', "The schema for the results of the |UDF|, indicating the new column(s) to "
    #      "add.  Each tuple provides the column name and data type, and is of the form (str, type).")
    # @arg('columns_accessed', list, "List of columns which the |UDF| will access.  This adds significant performance "
    #      "benefit if we know which column(s) will be needed to execute the |UDF|, especially when the frame has "
    #      "significantly more columns than those being used to evaluate the |UDF|.")

def add_columns(self, func, schema, columns_accessed=None):
    """
    Add columns to current frame.

    Assigns data to column based on evaluating a function for each row.

    Notes
    -----

    1.  The row |UDF| ('func') must return a value in the same format as
        specified by the schema.
        See :doc:`/ds_apir`.
    +   Unicode in column names is not supported and will likely cause the
        drop_frames() method (and others) to fail!

    Parameters
    ----------

    :param func: (UDF) Function which takes the values in the row and produces a value, or collection of values, for the new cell(s).
    :param schema: (List[(str,type)]) Schema for the column(s) being added.
    :param columns_accessed: (Optional[List[str]]) List of columns which the UDF will access. This adds significant
            performance benefit if we know which column(s) will be needed to execute the UDF, especially when the
            frame has significantly more columns than those being used to evaluate the UDF.


    Examples
    --------

    Given our frame, let's add a column which has how many years the person has been over 18

        >>> frame = tc.frame.create([['Fred',39,16,'555-1234'],
        ...                          ['Susan',33,3,'555-0202'],
        ...                          ['Thurston',65,26,'555-4510'],
        ...                          ['Judy',44,14,'555-2183']],
        ...                         schema=[('name', str), ('age', int), ('tenure', int), ('phone', str)])

        >>> frame.inspect()
        [#]  name      age  tenure  phone
        ====================================
        [0]  Fred       39      16  555-1234
        [1]  Susan      33       3  555-0202
        [2]  Thurston   65      26  555-4510
        [3]  Judy       44      14  555-2183

        >>> frame.add_columns(lambda row: row.age - 18, ('adult_years', int))

        >>> frame.inspect()
        [#]  name      age  tenure  phone     adult_years
        =================================================
        [0]  Fred       39      16  555-1234           21
        [1]  Susan      33       3  555-0202           15
        [2]  Thurston   65      26  555-4510           47
        [3]  Judy       44      14  555-2183           26


    Multiple columns can be added at the same time.  Let's add percentage of
    life and percentage of adult life in one call, which is more efficient.

        >>> frame.add_columns(lambda row: [row.tenure / float(row.age), row.tenure / float(row.adult_years)],
        ...                   [("of_age", float), ("of_adult", float)])

        >>> frame.inspect(round=2)
        [#]  name      age  tenure  phone     adult_years  of_age  of_adult
        ===================================================================
        [0]  Fred       39      16  555-1234           21    0.41      0.76
        [1]  Susan      33       3  555-0202           15    0.09      0.20
        [2]  Thurston   65      26  555-4510           47    0.40      0.55
        [3]  Judy       44      14  555-2183           26    0.32      0.54

    Note that the function returns a list, and therefore the schema also needs to be a list.

    It is not necessary to use lambda syntax, any function will do, as long as it takes a single row argument.  We
    can also call other local functions within.

    Let's add a column which shows the amount of person's name based on their adult tenure percentage.

        >>> def percentage_of_string(string, percentage):
        ...     '''returns a substring of the given string according to the given percentage'''
        ...     substring_len = int(percentage * len(string))
        ...     return string[:substring_len]

        >>> def add_name_by_adult_tenure(row):
        ...     return percentage_of_string(row.name, row.of_adult)

        >>> frame.add_columns(add_name_by_adult_tenure, ('tenured_name', unicode))

        <skip>
        >>> frame
        Frame "example_frame"
        row_count = 4
        schema = [name:unicode, age:int32, tenure:int32, phone:unicode, adult_years:int32, of_age:float32, of_adult:float32, tenured_name:unicode]
        status = ACTIVE  (last_read_date = -etc-)
        </skip>

        >>> frame.inspect(columns=['name', 'of_adult', 'tenured_name'], round=2)
        [#]  name      of_adult  tenured_name
        =====================================
        [0]  Fred          0.76  Fre
        [1]  Susan         0.20  S
        [2]  Thurston      0.55  Thur
        [3]  Judy          0.54  Ju


    **Optimization** - If we know up front which columns our row function will access, we
    can tell add_columns to speed up the execution by working on only the limited feature
    set rather than the entire row.

    Let's add a name based on tenure percentage of age.  We know we're only going to use
    columns 'name' and 'of_age'.

        >>> frame.add_columns(lambda row: percentage_of_string(row.name, row.of_age),
        ...                   ('tenured_name_age', unicode),
        ...                   columns_accessed=['name', 'of_age'])

        >>> frame.inspect(round=2)
        [#]  name      age  tenure  phone     adult_years  of_age  of_adult
        ===================================================================
        [0]  Fred       39      16  555-1234           21    0.41      0.76
        [1]  Susan      33       3  555-0202           15    0.09      0.20
        [2]  Thurston   65      26  555-4510           47    0.40      0.55
        [3]  Judy       44      14  555-2183           26    0.32      0.54
        <blankline>
        [#]  tenured_name  tenured_name_age
        ===================================
        [0]  Fre           F
        [1]  S
        [2]  Thur          Thu
        [3]  Ju            J

    More information on a row |UDF| can be found at :doc:`/ds_apir`

    """
    # For further examples, see :ref:`example_frame.add_columns`.

    is_mergeable(self._tc, self.schema, schema)

    row = Row(self.schema)

    def add_columns_func(r):
        row._set_data(r)
        return func(row)
    if isinstance(schema, list):
        self._python.rdd = self._python.rdd.map(lambda r: r + add_columns_func(r))
        self._python.schema.extend(schema)
    else:
        self._python.rdd = self._python.rdd.map(lambda r: r + [add_columns_func(r)])
        self._python.schema.append(schema)
