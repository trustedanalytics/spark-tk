from sparktk.frame.row import Row

def count(self, where):
    """
    Counts qualified rows.

    :param where: |UDF| which evaluates a row to a boolean
    :return: Number of rows matching qualifications.

    Counts rows which meet criteria specified by a UDF predicate.

    Examples
    --------

    >>> frame = tc.to_frame([['Fred',39,16,'555-1234'],
    ...                      ['Susan',33,3,'555-0202'],
    ...                      ['Thurston',65,26,'555-4510'],
    ...                      ['Judy',44,14,'555-2183']],
    ...                     schema=[('name', str), ('age', int), ('tenure', int), ('phone', str)])

    >>> frame.inspect()
    [#]  name      age  tenure  phone
    ====================================
    [0]  Fred       39      16  555-1234
    [1]  Susan      33       3  555-0202
    [2]  Thurston   65      26  555-4510
    [3]  Judy       44      14  555-2183

    >>> frame.count(lambda row: row.age > 35)
    3

    """
    row = Row(self.schema)

    def count_where(r):
        row._set_data(r)
        return where(row)

    return self._python.rdd.filter(lambda r: count_where(r)).count()
