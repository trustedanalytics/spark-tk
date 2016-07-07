"""
definitions for Data Types
"""

# TODO - consider server providing types, similar to commands

__all__ = ['dtypes', 'ignore', 'unknown', 'float32', 'float64', 'int32', 'int64', 'vector', 'unit', 'datetime']

import numpy as np
import json
import re
from pyspark.sql import types
import logging
logger = logging.getLogger('sparktk')

# alias numpy types
# todo: bring back the numpy types
# this is temporary until the serialization gets completely figured out...
# float32 = np.float32
# float64 = np.float64
# int32 = np.int32
# int64 = np.int64
float32 = float
float64 = float
int32 = int
int64 = long

from datetime import datetime
import dateutil.parser as datetime_parser
import pytz
# Chose python's datetime over numpy.datetime64 because of time zone support and string serialization
# Here's a long thread discussing numpy's datetime64 timezone problem:
#   http://mail.scipy.org/pipermail/numpy-discussion/2013-April/066038.html
# If need be, UDFs can create numpy objects from x using: numpy.datatime64(x.isoformat())


class _Vector(object):

    base_type = np.ndarray
    re_pattern = re.compile(r"^vector\((\d+)\)$")

    def __init__(self, length):
        self.length = int(length)
        self.constructor = self._get_constructor()

    def _get_constructor(self):
        length = self.length

        def constructor(value):
            """
            Creates a numpy array from a value, which can be one of many types
            """
            if value is None:
                return None
            try:
                # first try numpy's constructor
                array = np.array(value, dtype=np.float64)  # ensures the array is entirely made of doubles
            except:
                # also support json or comma-sep string
                if dtypes.value_is_string(value):
                    try:
                        value = json.loads(value)
                    except:
                        value = [np.float64(item.strip()) for item in value.split(',') if item]
                    array = np.array(value, dtype=np.float64)  # ensures the array is entirely made of doubles
                else:
                    raise

            array = np.atleast_1d(array)  # numpy thing, so that vectors of size 1 will still have dimension and length
            if len(array) != length:
                raise ValueError("Could not construct vector in Python Client.  Expected vector of length %s, but received length %d" % (length, len(array)))
            return array
        return constructor

    @property
    def is_complex_type(self):
        return True

    @staticmethod
    def get_from_string(data_type_str):
        return _Vector(_Vector.re_pattern.match(data_type_str).group(1))

    @property
    def is_complex_type(self):
        return True

    def __repr__(self):
        return "vector(%d)" % self.length

vector = _Vector


class _Unit(object):
    """Ignore type used for schemas during file import"""
    pass

unit = _Unit


class _Ignore(object):
    """Ignore type used for schemas during file import"""
    pass

ignore = _Ignore


class _Unknown(object):
    """Unknown type used when type is indeterminate"""
    pass

unknown = _Unknown


# map types to their string identifier
_primitive_type_to_str_table = {
    #bool: "bool", TODO
    #bytearray: "bytearray", TODO
    #dict: "dict", TODO
    float32: "float32",
    float64: "float64",
    int32: "int32",
    int64: "int64",
    #list: "list", TODO
    unicode: "unicode",
    ignore: "ignore",
    datetime: "datetime",
}

# map the pyspark sql type to the primitive data type
_pyspark_type_to_primitive_type_table = {
    #types.BooleanType : bool,
    types.LongType : int,
    types.IntegerType : int,
    types.DoubleType : float,
    types.DecimalType : float,
    types.StringType : str,
    types.TimestampType : datetime
}

# map data type to pyspark sql type
_data_type_to_pyspark_type_table = {
    int: types.IntegerType(),
    long: types.LongType(),
    float: types.DoubleType(),
    str: types.StringType(),
    datetime: types.TimestampType()
}

# build reverse map string -> type
_primitive_str_to_type_table = dict([(s, t) for t, s in _primitive_type_to_str_table.iteritems()])

_primitive_alias_type_to_type_table = {
    float: float64,
    int: int32,
    long: int64,
    str: unicode,
    list: vector,
}

_primitive_alias_str_to_type_table = dict([(alias.__name__, t) for alias, t in _primitive_alias_type_to_type_table.iteritems()])
_primitive_alias_str_to_type_table["string"] = unicode


def get_float_constructor(float_type):
    """Creates special constructor for floating point types which handles nan, inf, -inf"""
    ft = float_type

    def float_constructor(value):
        result = ft(value)
        if np.isnan(result) or result == np.inf or result == -np.inf:  # this is 5x faster than calling np.isfinite()
            return None
        return ft(value)
    return float_constructor

def datetime_to_ms(date_time):
    """
    Returns the number of milliseconds since epoch (1970-01-01).
    :param date_time: (datetime) Date/time to convert to timestamp
    :return: Timestamp (number of ms since epoch)
    """
    if isinstance(date_time, datetime):
        ms = long(date_time.strftime("%s")) * 1000.0
        ms += date_time.microsecond // 1000
        return ms
    else:
        raise TypeError("Unable to calculate the number of milliseconds since epoch for type: %s" % type(date_time))

def ms_to_datetime_str(ms):
    """
    Returns the date/time string for the specified timestamp (milliseconds since epoch).
    :param ms: Milliseconds since epoch (int or long)
    :return: Date/time string
    """
    if isinstance(ms, long) or isinstance(ms, int):
        return datetime.fromtimestamp(ms/1000.0, tz=pytz.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    else:
        raise TypeError("Unable to convert timestamp milliseconds to a date/time string, because the value provided " +
                        "is not a long/int.  Unsupported type: %" % type(ms))

def datetime_constructor(value):
    """Creates special constructor for datetime parsing.  Returns the number of ms since epoch."""
    if dtypes.value_is_string(value):
        return datetime_to_ms(datetime_parser.parse(value))
    elif isinstance(value, long) or isinstance(value, int):
        return value
    else:
        try:
            return datetime_to_ms(datetime(*value))
        except:
            raise TypeError("cannot convert type to the datetime")


def numpy_to_bson_friendly(obj):
    """take an object and convert it to a type that can be serialized to bson if neccessary."""
    if isinstance(obj, float32) or isinstance(obj, float64):
        return float(obj)
    if isinstance(obj, int32):
        return int(obj)
    if isinstance(obj, vector.base_type):
        return obj.tolist()
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, dict):
        return dict([(numpy_to_bson_friendly(key), numpy_to_bson_friendly(value)) for key, value in obj.items()])
    # Let the base class default method raise the TypeError
    return obj


class _DataTypes(object):
    """
    Provides functions with define and operate on supported data types.
    """

    def __contains__(self, item):
        try:
            self.validate(item)
            return True
        except ValueError:
            return False

    def __repr__(self):
        aliases = "\n(and aliases: %s)" % (", ".join(sorted(["%s->%s" % (alias.__name__, self.to_string(data_type)) for alias, data_type in _primitive_alias_type_to_type_table.iteritems()])))
        return ", ".join(sorted(_primitive_str_to_type_table.keys() + ["vector(n)"])) + aliases

    @staticmethod
    def value_is_string(value):
        """get bool indication that value is a string, whether str or unicode"""
        return isinstance(value, basestring)

    @staticmethod
    def value_is_missing_value(value):
        return value is None or (type(value) in [float32, float64, float] and (np.isnan(value) or value in [np.inf, -np.inf]))

    @staticmethod
    def get_primitive_data_types():
        return _primitive_type_to_str_table.keys()

    @staticmethod
    def to_string(data_type):
        """
        Returns the string representation of the given type

        Parameters
        ----------
        data_type : type
            valid data type; if invalid, a ValueError is raised

        Returns
        -------
        result : str
            string representation

        Examples
        --------
        >>> dtypes.to_string(float32)
        'float32'
        """
        valid_data_type = _DataTypes.get_from_type(data_type)
        try:
            return _primitive_type_to_str_table[valid_data_type]
        except KeyError:
            # complex data types should use their repr
            return repr(valid_data_type)

    @staticmethod
    def get_from_string(data_type_str):
        """
        Returns the data type for the given type string representation

        Parameters
        ----------
        data_type_str : str
            valid data type str; if invalid, a ValueError is raised

        Returns
        -------
        result : type
            type represented by the string

        Examples
        --------
        >>> dtypes.get_from_string('unicode')
        unicode
        """
        try:
            return _primitive_str_to_type_table[data_type_str]
        except KeyError:
            try:
                return _primitive_alias_str_to_type_table[data_type_str]
            except KeyError:
                try:
                    return vector.get_from_string(data_type_str)
                except:
                    raise ValueError("Unsupported type string '%s' " % data_type_str)

    @staticmethod
    def is_primitive_type(data_type):
        return data_type in _primitive_type_to_str_table or data_type in _primitive_alias_type_to_type_table

    @staticmethod
    def is_complex_type(data_type):
        try:
            return data_type.is_complex_type
        except AttributeError:
            return False

    @staticmethod
    def is_primitive_alias_type(data_type):
        return data_type in _primitive_alias_type_to_type_table

    @staticmethod
    def is_int(data_type):
        return data_type in [int, int32, int64]

    @staticmethod
    def is_float(data_type):
        return data_type in [float, float32, float64]

    @staticmethod
    def get_from_type(data_type):
        """
        Returns the data type for the given type (often it will return the same type)

        Parameters
        ----------
        data_type : type
            valid data type or type that may be aliased for a valid data type;
            if invalid, a ValueError is raised

        Returns
        -------
        result : type
            valid data type for given type

        Examples
        --------
        >>> dtypes.get_from_type(int)
        numpy.int32
        """
        if _DataTypes.is_primitive_alias_type(data_type):
            return _primitive_alias_type_to_type_table[data_type]
        if _DataTypes.is_primitive_type(data_type) or _DataTypes.is_complex_type(data_type):
            return data_type
        raise ValueError("Unsupported type %s" % data_type)

    @staticmethod
    def validate(data_type):
        """Raises a ValueError if data_type is not a valid data_type"""
        _DataTypes.get_from_type(data_type)

    @staticmethod
    def get_constructor(to_type):
        """gets the constructor for the to_type"""
        try:
            return to_type.constructor
        except AttributeError:
            if to_type == float64 or to_type == float32:
                return get_float_constructor(to_type)
            if to_type == datetime:
                return datetime_constructor

            def constructor(value):
                if value is None:
                    return None
                try:
                    return to_type(value)
                except Exception as e:
                    print "ERROR for value %s.  %s" % (value, e)
                    raise
            return constructor

    @staticmethod
    def cast(value, to_type):
        """
        Returns the given value cast to the given type.  None is always returned as None

        Parameters
        ----------
        value : object
            value to convert by casting

        to_type : type
            valid data type to use for the cast

        Returns
        -------
        results : object
            the value cast to the to_type

        Examples
        --------
        >>> dtypes.cast(3, float64)
        3.0
        >>> dtypes.cast(4.5, str)
        '4.5'
        >>> dtypes.cast(None, str)
        None
        >>> dtypes.cast(np.inf, float32)
        None
        """
        if _DataTypes.value_is_missing_value(value):  # Special handling for missing values
            return None
        elif _DataTypes.is_primitive_type(to_type) and type(value) is to_type:  # Optimization
            return value
        try:
            constructor = _DataTypes.get_constructor(to_type)
            result = constructor(value)
            return None if _DataTypes.value_is_missing_value(result) else result
        except Exception as e:
            raise ValueError(("Unable to cast to type %s\n" % to_type) + str(e))

    @staticmethod
    def datetime_from_iso(iso_string):
        """create datetime object from ISO 8601 string"""
        return datetime_parser.parse(iso_string)

    @staticmethod
    def get_primitive_type_from_pyspark_type(pyspark_type):
        """
        Get the primitive type for the specified pyspark sql data type.
        
        :param pyspark_type: pyspark.sql.types data type
        :return: Primitive data type
        """
        if _pyspark_type_to_primitive_type_table.has_key(pyspark_type):
            return _pyspark_type_to_primitive_type_table[pyspark_type]
        else:
            raise ValueError("Unable to cast pyspark type %s to primitive type." % str(pyspark_type))

    @staticmethod
    def merge_types (type_a, type_b):
        """
        Returns the lowest common denominator data type for the specified types.
        :param type_a: Data type a to compare
        :param type_b: Data type b t compare
        :return: Merged data type
        """
        merged = unicode
        numeric_types = [float, long, int, bool]       # numeric types in rank order
        if type_a == type_b:
            merged = type_a
        elif type_a == unicode or type_b == unicode:
            merged = unicode
        elif type_a == str or type_b == str:
            merged = str
        elif type_a in numeric_types and type_b in numeric_types:
            if numeric_types.index(type_a) > numeric_types.index(type_b):
                merged = type_b
            else:
                merged = type_a
        elif isinstance(type_a, vector) and isinstance(type_b, vector):
            if type_a.length != type_b.length:
                raise ValueError("Vectors must all be the same length (found vectors with length %s and %s)." % (type_a.length, type_b.length))
            merged = type_a
        return merged


dtypes = _DataTypes()
#valid_data_types = dtypes  # consider for backwards comp

