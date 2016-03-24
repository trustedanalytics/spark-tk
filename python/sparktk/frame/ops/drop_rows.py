from sparktk.frame.row import Row

def drop_rows(self, predicate):
    """
    Erase any row in the current frame which qualifies.

    Examples
    --------

    .. code::

        >>> frame = tk_context.to_frame([['Fred',39,16,'555-1234'],
        ...                              ['Susan',33,3,'555-0202'],
        ...                              ['Thurston',65,26,'555-4510'],
        ...                              ['Judy',44,14,'555-2183']],
        ...                             schema=[('name', str), ('age', int), ('tenure', int), ('phone', str)])

        >>> frame.inspect()
        [#]  name      age  tenure  phone
        ====================================
        [0]  Fred       39      16  555-1234
        [1]  Susan      33       3  555-0202
        [2]  Thurston   65      26  555-4510
        [3]  Judy       44      14  555-2183

        >>> frame.drop_rows(lambda row: row.name[-1] == 'n')  # drop people whose name ends in 'n'

        >>> frame.inspect()
        [#]  name  age  tenure  phone
        ================================
        [0]  Fred   39      16  555-1234
        [1]  Judy   44      14  555-2183

    More information on a |UDF| can be found at :doc:`/ds_apir`.
    """
    row = Row(self.schema)

    def drop_rows_func(r):
        row._set_data(r)
        return not predicate(row)
    self._python.rdd = self._python.rdd.filter(drop_rows_func)

