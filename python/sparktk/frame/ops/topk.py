
def top_k(self, column_name, k, weight_column=None):
    """
    Most or least frequent column values.

    Parameters
    ----------

    :param column_name: (str) The column whose top (or bottom) K distinct values are to be calculated.
    :param k: (int) Number of entries to return (If k is negative, return bottom k).
    :param weight_column: (Optional[str]) The column that provides weights (frequencies) for the topK calculation.
                          Must contain numerical data. Default is 1 for all items.

    Calculate the top (or bottom) K distinct values by count of a column. The column can be
    weighted.  All data elements of weight <= 0 are excluded from the calculation, as are
    all data elements whose weight is NaN or infinite. If there are no data elements of
    finite weight > 0, then topK is empty.

    Examples
    --------

    For this example, we calculate the top 2 counties in a data frame:

    <hide>
    >>> frame = tc.frame.create([[1, "Portland", 609456, 583776, "4.40%", "Multnomah" ],
    ...                          [2, "Salem", 160614, 154637, "3.87%", "Marion" ],
    ...                          [3, "Eugene", 159190, 156185, "1.92%", "Lane" ],
    ...                          [4, "Gresham", 109397, 105594, "3.60%", "Multnomah" ],
    ...                          [5, "Hillsboro", 97368, 91611, "6.28%", "Washington" ],
    ...                          [6, "Beaverton", 93542, 89803, "4.16%", "Washington" ],
    ...                          [15, "Grants Pass", 35076, 34533, "1.57%", "Josephine" ],
    ...                          [16, "Oregon City", 34622, 31859, "8.67%", "Clackamas" ],
    ...                          [17, "McMinnville", 33131, 32187, "2.93%", "Yamhill" ],
    ...                          [18, "Redmond", 27427, 26215, "4.62%", "Deschutes" ],
    ...                          [19, "Tualatin", 26879, 26054, "4.17%", "Washington" ],
    ...                          [20, "West Linn", 25992, 25109, "3.52%", "Clackamas" ],
    ...                          [7, "Bend", 81236, 76639, "6.00%", "Deschutes" ],
    ...                          [8, "Medford", 77677, 74907, "3.70%", "Jackson" ],
    ...                          [9, "Springfield", 60177, 59403, "1.30%", "Lane" ],
    ...                          [10, "Corvallis", 55298, 54462, "1.54%", "Benton" ],
    ...                          [11, "Albany", 51583, 50158, "2.84%", "Linn" ],
    ...                          [12, "Tigard", 50444, 48035, "5.02%", "Washington" ],
    ...                          [13, "Lake Oswego", 37610, 36619, "2.71%", "Clackamas" ],
    ...                          [14, "Keizer", 37064,36478, "1.61%", "Marion" ]],
    ...                         [('rank', int), ('city', str), ('population_2013', int), ('population_2010',int), ('change',str), ('county',str)])
    -etc-

    </hide>

    Consider the following frame:

        >>> frame.inspect(frame.row_count)
        [##]  rank  city         population_2013  population_2010  change  county
        =============================================================================
        [0]      1  Portland              609456           583776  4.40%   Multnomah
        [1]      2  Salem                 160614           154637  3.87%   Marion
        [2]      3  Eugene                159190           156185  1.92%   Lane
        [3]      4  Gresham               109397           105594  3.60%   Multnomah
        [4]      5  Hillsboro              97368            91611  6.28%   Washington
        [5]      6  Beaverton              93542            89803  4.16%   Washington
        [6]     15  Grants Pass            35076            34533  1.57%   Josephine
        [7]     16  Oregon City            34622            31859  8.67%   Clackamas
        [8]     17  McMinnville            33131            32187  2.93%   Yamhill
        [9]     18  Redmond                27427            26215  4.62%   Deschutes
        [10]    19  Tualatin               26879            26054  4.17%   Washington
        [11]    20  West Linn              25992            25109  3.52%   Clackamas
        [12]     7  Bend                   81236            76639  6.00%   Deschutes
        [13]     8  Medford                77677            74907  3.70%   Jackson
        [14]     9  Springfield            60177            59403  1.30%   Lane
        [15]    10  Corvallis              55298            54462  1.54%   Benton
        [16]    11  Albany                 51583            50158  2.84%   Linn
        [17]    12  Tigard                 50444            48035  5.02%   Washington
        [18]    13  Lake Oswego            37610            36619  2.71%   Clackamas
        [19]    14  Keizer                 37064            36478  1.61%   Marion

        >>> top_frame = frame.top_k("county", 2)
        <progress>

        >>> top_frame.inspect()
        [#]  county      count
        ======================
        [0]  Washington    4.0
        [1]  Clackamas     3.0

    """
    from sparktk.frame.frame import Frame
    return Frame(self._tc, self._scala.topK(column_name, k, self._tc.jutils.convert.to_scala_option(weight_column)))