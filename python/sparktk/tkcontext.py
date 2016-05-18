from lazyloader import get_lazy_loader
from sparktk.jvm.jutils import JUtils
from sparktk.sparkconf import create_sc
from sparktk.loggers import loggers
from pyspark import SparkContext

class TkContext(object):
    """TK Context - grounding object for the sparktk library"""

    def __init__(self, sc=None, **create_sc_kwargs):
        if not sc:
            sc = create_sc(**create_sc_kwargs)
        if type(sc) is not SparkContext:
            raise TypeError("sparktk context init requires a valid SparkContext.  Received type %s" % type(sc))
        self._sc = sc
        self._jtc = self._sc._jvm.org.trustedanalytics.sparktk.TkContext(self._sc._jsc)
        self._jutils = JUtils(self._sc)
        loggers.set_spark(self._sc, "off")  # todo: undo this/move to config, I just want it outta my face most of the time

    @property
    def sc(self):
        return self._sc

    @property
    def jutils(self):
        return self._jutils

    @property
    def models(self):
        """access to the various models of sparktk"""
        return get_lazy_loader(self, "models")

    def to_frame(self, data, schema=None):
        """creates a frame from the given data"""
        from sparktk.frame.frame import to_frame
        return to_frame(self, data, schema)

    def load_frame(self, path):
        """loads a previously saved frame"""
        from sparktk.frame.frame import load_frame
        return load_frame(self, path)

    def load_frame_from_csv(self, path, delimiter=",", header=False, inferschema=True, schema=None):
        """
        Creates a frame with data from a csv file.

        :param path: Full path to the csv file
        :param delimiter: A string which indicates the separation of data fields.  This is usually a single
                          character and could be a non-visible character, such as a tab. The default delimiter
                          is a comma (,).
        :param header: Boolean value indicating if the first line of the file will be used to name columns,
                       and not be included in the data.  The default value is false.
        :param inferschema: Boolean value indicating if the column types will be automatically inferred.  It
                            requires one extra pass over the data and is false by default.
        :param: schema: Optionally specify the schema for the dataset.  Number of columns specified in the
                        schema must match the number of columns in the csv file provided.
        :return: Frame that contains the data from the csv file

        Examples
        --------

        Load a frame from a csv file by specifying the path to the file, delimiter, and options that specify that
        there is a header and to infer the schema based on the data.

        .. code::

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
        from sparktk.frame.frame import load_frame_from_csv
        return load_frame_from_csv(self, path, delimiter, header, inferschema, schema)