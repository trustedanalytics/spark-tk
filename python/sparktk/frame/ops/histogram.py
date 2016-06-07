from sparktk.propobj import PropertiesObject

class Histogram(PropertiesObject):
    def __init__(self, cutoffs, hist, density):
        self._cutoffs = cutoffs
        self._hist = hist
        self._density = density

    @property
    def cutoffs(self):
        return self._cutoffs

    @property
    def density(self):
        return self._density

    @property
    def hist(self):
        return self._hist


def histogram(self, column_name, num_bins=None, weight_column_name=None, bin_type="equalwidth"):
    """
    Compute the histogram for a column in a frame.

    The returned value is a Histogram object containing 3 lists one each for:
    the cutoff points of the bins, size of each bin, and density of each bin.

    **Notes**

    The num_bins parameter is considered to be the maximum permissible number
    of bins because the data may dictate fewer bins.
    With equal depth binning, for example, if the column to be binned has 10
    elements with only 2 distinct values and the *num_bins* parameter is
    greater than 2, then the number of actual number of bins will only be 2.
    This is due to a restriction that elements with an identical value must
    belong to the same bin.

    Examples
    --------
    <hide>
        >>> data = [['a', 2], ['b', 7], ['c', 3], ['d', 9], ['e', 1]]
        >>> schema = [('a', str), ('b', int)]

        >>> frame = tc.frame.create(data, schema)
        <progress>

    </hide>

    Consider the following sample data set\:

    .. code::

        >>> frame.inspect()
            [#]  a  b
            =========
            [0]  a  2
            [1]  b  7
            [2]  c  3
            [3]  d  9
            [4]  e  1

    A simple call for 3 equal-width bins gives\:

    .. code::

        >>> hist = frame.histogram("b", num_bins=3)
        <progress>
        >>> hist.cutoffs
        [1.0, 3.6666666666666665, 6.333333333333333, 9.0]

        >>> hist.hist
        [3.0, 0.0, 2.0]

        >>> hist.density
        [0.6, 0.0, 0.4]

    Switching to equal depth gives\:

    .. code::

        >>> hist = frame.histogram("b", num_bins=3, bin_type='equaldepth')
        <progress>

        >>> hist.cutoffs
        [1.0, 2.0, 7.0, 9.0]

        >>> hist.hist
        [1.0, 2.0, 2.0]

        >>> hist.density
        [0.2, 0.4, 0.4]

    .. only:: html

           Plot hist as a bar chart using matplotlib\:

        .. code::
    <skip>
            >>> import matplotlib.pyplot as plt

            >>> plt.bar(hist,cutoffs[:1], hist.hist, width=hist.cutoffs[1] - hist.cutoffs[0])
    </skip>
    .. only:: latex

           Plot hist as a bar chart using matplotlib\:

        .. code::
    <skip>
            >>> import matplotlib.pyplot as plt

            >>> plt.bar(hist.cutoffs[:1], hist.hist, width=hist.cutoffs[1] -
            ... hist["cutoffs"][0])
    </skip>


    :param column_name: Name of column to be evaluated.
    :param num_bins: Number of bins in histogram.
                     Default is Square-root choice will be used
                     (in other words math.floor(math.sqrt(frame.row_count)).
    :param weight_column_name: Name of column containing weights.
                               Default is all observations are weighted equally.
    :param bin_type: The type of binning algorithm to use: ["equalwidth"|"equaldepth"]
                     Defaults is "equalwidth".
    :return: histogram
                A Histogram object containing the result set.
                The data returned is composed of multiple components:
            cutoffs : array of float
                A list containing the edges of each bin.
            hist : array of float
                A list containing count of the weighted observations found in each bin.
            density : array of float
                A list containing a decimal containing the percentage of
                observations found in the total set per bin.
    """
    results = self._tc.jutils.convert.scala_map_string_seq_to_python(self._scala.histogram(column_name,
                          self._tc.jutils.convert.to_scala_option(num_bins),
                          self._tc.jutils.convert.to_scala_option(weight_column_name),
                          bin_type))
    return Histogram(**results)

