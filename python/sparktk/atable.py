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

"""
Pretty-printing tabular data
"""

from datetime import datetime

spaces_between_cols = 2  # consts
ellipses = '...'


class Formatting(object):
    """Format settings for the pretty printing a table method

    wrap: int or 'stripes'
      If set to 'stripes' then table rows print in stripes; if set to an integer N,
      rows will be printed in clumps of N columns, where the columns are wrapped")

    truncate: int
      If set to integer N, all strings will be truncated to length N, including a tagged ellipses

    round: int
      If set to integer N, all floating point numbers will be rounded and truncated to N digits

    width: int
      If set to integer N, the print out will try to honor a max line width of N

    margin: int (only has meaning in 'stripes' mode)
      If set to integer N, the margin for printing names in a stripe will be limited to N characters

    with_types: bool
      If set to True, header will include the data_type of each column
    """

    _unspecified = 'atable_formatting'  # sentinel

    _default_wrap = 20
    _default_truncate = None
    _default_round = None
    _default_width = 80
    _default_margin = None
    _default_with_types = False

    def __init__(self, wrap=None, truncate=None, round=None, width=None, margin=None, with_types=None):
        self._wrap = None
        self.wrap = wrap
        self._truncate = None
        self.truncate = truncate
        self._round = None
        self.round = round
        self._width = None
        self.width = width
        self._margin = None
        self.margin = margin
        self._with_types = None
        self.with_types = with_types

    def reset(self):
        """returns all the settings to their default values"""
        # the setters use None to restore to default
        self.wrap = None
        self.truncate = None
        self.round = None
        self.width = None
        self.margin = None
        self.with_types = None

    def copy(self,
             wrap=_unspecified,
             truncate=_unspecified,
             round=_unspecified,
             width=_unspecified,
             margin=_unspecified,
             with_types=_unspecified):
        """create a copy of this settings object and override any values specified"""
        c = Formatting(self.wrap, self.truncate, self.round, self.width, self.margin, self.with_types)
        if wrap is not self._unspecified:
            c.wrap = wrap
        if truncate is not self._unspecified:
            c.truncate = truncate
        if round is not self._unspecified:
            c.round = round
        if width is not self._unspecified:
            c.width = width
        if margin is not self._unspecified:
            c.margin = margin
        if with_types is not self._unspecified:
            c.with_types = with_types
        return c

    def __repr__(self):
        """displays current settings"""
        return """wrap       %8s
truncate   %8s
round      %8s
width      %8s
margin     %8s
with_types %8s""" % (self.wrap, self.truncate, self.round, self.width, self.margin, self.with_types)

    @property
    def wrap(self):
        """
        If set to 'stripes' then talbe rows print in stripes; if set to an integer N,
        rows will be printed in clumps of N columns, where the columns are wrapped")
        """
        return self._wrap

    @wrap.setter
    def wrap(self, value):
        supported_strings = ['stripes']
        if value is None:
            value = self._default_wrap
        if not isinstance(value, (basestring, int, long)) or \
                (isinstance(value, basestring) and value not in supported_strings) or \
                (isinstance(value, (int, long)) and value <= 0):
            raise ValueError("Bad value %s.  wrap must be a integer > 0 or one of the following strings: %s" %
                             (value, ", ".join(supported_strings)))
        self._wrap = value

    @property
    def truncate(self):
        """If set to integer N, all strings will be truncated to length N, including a tagged ellipses"""
        return self._truncate

    @truncate.setter
    def truncate(self, value):
        if value is None:
            value = self._default_truncate
        if value is not None and (not isinstance(value, (int, long)) or value <= 0):
            raise ValueError("Bad value %s.  truncate must be a integer > 0" % value)
        self._truncate = value

    @property
    def round(self):
        """If set to integer N, all floating point numbers will be rounded and truncated to N digits"""
        return self._round

    @round.setter
    def round(self, value):
        if value is None:
            value = self._default_round
        if value is not None and (not isinstance(value, (int, long)) or value < 0):
            raise ValueError("Bad value %s.  round must be an integer >= 0" % value)
        self._round = value

    @property
    def width(self):
        """If set to integer N, the print out will try to honor a max line width of N"""
        return self._width

    @width.setter
    def width(self, value):
        if value is None:
            value = self._default_width
        if not isinstance(value, (int, long)) or value <= 0:
            raise ValueError("Bad value %s.  width must be an integer >= 0" % value)
        self._width = value

    @property
    def margin(self):
        """
        (only has meaning in wrap='stripes' mode)
        If set to integer N, the margin for printing names in a stripe will be limited to N characters
        """
        return self._margin

    @margin.setter
    def margin(self, value):
        if value is None:
            value = self._default_margin
        if value is not None and (not isinstance(value, (int, long)) or value <= 0):
            raise ValueError("Bad value %s.  margin must be an integer >= 0" % value)
        self._margin = value

    @property
    def with_types(self):
        """If set to True, header will include the data_type of each column"""
        return self._with_types

    @with_types.setter
    def with_types(self, value):
        if value is None:
            value = self._default_with_types
        if not isinstance(value, bool):
            raise ValueError("Bad value %s.  with_types must be an integer >= 0" % value)
        self._with_types = value

inspect_settings = Formatting()


class ATable(object):
    """
    Class for representing tabular data as a string, where the __repr__ is the main use case
    """

    def __init__(self, rows, schema, offset, format_settings=None):
        if not format_settings:
            format_settings = Formatting()
        if not isinstance(format_settings, Formatting):
            raise TypeError("argument format_settings must be type %s" % Formatting)
        if format_settings.wrap == 'stripes':
            self._repr = self._repr_stripes
        else:
            self.wrap = min(format_settings.wrap, len(rows)) or len(rows)
            self._repr = self._repr_wrap

        self.rows = rows
        self.schema = schema
        self.offset = offset
        self.truncate = format_settings.truncate
        self.round = format_settings.round
        self.width = format_settings.width
        self.margin = format_settings.margin
        self.with_types = format_settings.with_types
        self.value_formatters = [self._get_value_formatter(data_type) for name, data_type in schema]

    def __repr__(self):
        return self._repr()

    def _repr_wrap(self):
        """print rows in a 'clumps' style"""
        row_index_str_format = '[%s]' + ' ' * spaces_between_cols

        def _get_row_index_str(index):
            return row_index_str_format % index

        row_count = len(self.rows)
        row_clump_count = _get_row_clump_count(row_count, self.wrap)
        header_sizes = _get_header_entry_sizes(self.schema, self.with_types)
        column_spacer = ' ' * spaces_between_cols
        lines_list = []
        extra_tuples = []

        for row_clump_index in xrange(row_clump_count):
            if row_clump_index > 0:
                lines_list.append('')  # extra line for new clump
            start_row_index = row_clump_index * self.wrap
            stop_row_index = start_row_index + self.wrap
            if stop_row_index > row_count:
                stop_row_index = row_count
            row_index_header = _get_row_index_str('#' * len(str(self.offset+stop_row_index-1)))
            margin = len(row_index_header)
            col_sizes = _get_col_sizes(self.rows, start_row_index, self.wrap, header_sizes, self.value_formatters)
            col_index = 0
            while col_index < len(self.schema):
                num_cols = _get_num_cols(self.schema, self.width, col_index, col_sizes, margin)
                if num_cols == 0:
                    raise RuntimeError("Internal error, num_cols == 0")  # sanity check on algo
                header_line = row_index_header + column_spacer.join([pad_right(_get_header_entry(name, data_type, self.with_types), min(self.width - margin, col_sizes[col_index+i])) for i, (name, data_type) in enumerate(self.schema[col_index:col_index+num_cols])])
                thick_line = "=" * len(header_line)
                lines_list.extend(["", header_line, thick_line])
                if row_count:
                    for row_index in xrange(start_row_index, stop_row_index):
                        new_line = pad_right(_get_row_index_str(self.offset+row_index), margin) + column_spacer.join([self._get_wrap_entry(data, col_sizes[col_index+i], self.value_formatters[col_index+i], i, extra_tuples) for i, data in enumerate(self.rows[row_index][col_index:col_index+num_cols])])
                        lines_list.append(new_line.rstrip())
                        if extra_tuples:
                            lines_list.extend(_get_lines_from_extra_tuples(extra_tuples, col_sizes[col_index:col_index+num_cols], margin))
                col_index += num_cols

        return "\n".join(lines_list[1:])  # 1: skips the first blank line caused by the algo

    def _repr_stripes(self):
        """print rows as stripes style"""
        max_margin = 0
        for name, data_type in self.schema:
            length = len(_get_header_entry(name, data_type, self.with_types)) + 1 # to account for the '='
            if length > max_margin:
                max_margin = length
        if not self.margin or max_margin < self.margin:
            self.margin = max_margin
        lines_list = []
        rows = self.rows or [['' for entry in self.schema]]
        for row_index in xrange(len(rows)):
            lines_list.append(self._get_stripe_header(self.offset+row_index))
            lines_list.extend([self._get_stripe_entry(i, name, data_type, value)
                               for i, ((name, data_type), value) in enumerate(zip(self.schema, rows[row_index]))])
        return "\n".join(lines_list)

    def _get_stripe_header(self, index):
        row_number = "[%s]" % index
        return row_number + "-" * (self.margin - len(row_number))

    def _get_stripe_entry(self, i, name, data_type, value):
        entry = _get_header_entry(name, data_type, self.with_types)
        return "%s=%s" % (pad_right(entry, self.margin - 1), self.value_formatters[i](value))

    def _get_value_formatter(self, data_type):
        if self.round: # and is_type_float(data_type):
            rounder = self.get_rounder(data_type, self.round)
            if rounder:
                return rounder
        if hasattr(data_type, "get_atable_formatter"):
            return data_type.get_atable_formatter(self.round, self.truncate)
        if data_type is datetime:
            return self.get_datetime_formatter()
        if self.truncate and (data_type is str or data_type is unicode):
            return self.get_truncator(self.truncate)
        return identity

    @staticmethod
    def _get_wrap_entry(data, size, formatter, relative_column_index, extra_tuples):
        entry = unicode(formatter(data)).encode('utf-8')
        if isinstance(data, basestring):
            lines = entry.splitlines()
            if len(lines) > 1:
                entry = lines[0]  # take the first line now, and save the rest in an 'extra' tuple
                extra_tuples.append((relative_column_index, lines[1:]))
            return pad_right(entry, size)
        elif data is None or isinstance(data, list) or isinstance(data, tuple):
            return pad_right(entry, size)
        else:
            return pad_left(entry, size)

    @staticmethod
    def get_truncator(target_len):
        def truncate_string(s):
            return truncate(s, target_len)
        return truncate_string

    @staticmethod
    def get_rounder(float_type, num_digits):
        if hasattr(float_type, "get_atable_rounder"):
            return float_type.get_atable_rounder(num_digits)

        if hasattr(float_type, "round") or float_type is float:
            def rounder(value):
                if value is None:
                    return None
                template = "%%.%df" % num_digits
                return template % value
            return rounder

        return None

    def get_datetime_formatter(self):
        def format_datetime(d):
            if d is None:
                return None
            elif isinstance(d, long) or isinstance(d, int):
                return ms_to_datetime_str(d)
            elif isinstance(d, datetime):
                return d.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            else:
                return str(d)
        return format_datetime


def ms_to_datetime_str(ms):
    """
    Returns the date/time string for the specified timestamp (milliseconds since epoch).
    :param ms: Milliseconds since epoch (int or long)
    :return: Date/time string
    """
    try:
        import pytz
        tz =pytz.utc
    except ImportError:
        import sys
        sys.stderr.write("WARNING: pytz not installed, setting timezone argument to None")
        tz=None

    if isinstance(ms, long) or isinstance(ms, int):
        return datetime.fromtimestamp(ms/1000.0, tz=tz).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    else:
        raise TypeError("Unable to convert timestamp milliseconds to a date/time string, because the value provided " +
                        "is not a long/int.  Unsupported type: %" % type(ms))


def _get_header_entry(name, data_type, with_type):
    if with_type:
        return "%s:%s" % (name, type_to_simple_name(data_type))
    return name

def type_to_simple_name(data_type):
    try:
        return data_type.__name__ if data_type is not None else 'None'
    except AttributeError:
        return '<unknown>'

def _get_header_entry_sizes(schema, with_types):
    return [len(_get_header_entry(name, data_type, with_types)) for name, data_type in schema]

def pad_left(s, target_len):
    """pads string s on the left such that is has at least length target_len"""
    return ' ' * (target_len - len(s)) + s


def pad_right(s, target_len):
    """pads string s on the right such that is has at least length target_len"""
    return s + ' ' * (target_len - len(s))


def truncate(s, target_len):
    """truncates string to the target_len"""
    if target_len < len(ellipses):
        raise ValueError("Bad truncate length %s.  "
                         "Must be set to at least %s to allow for a '%s'." % (target_len, len(ellipses), ellipses))
    if s is None or len(s) <= target_len:
        return s
    return s[:target_len - len(ellipses)] + ellipses


def identity(value):
    return value


def _get_col_sizes(rows, row_index, row_count, header_sizes, formatters):
    sizes = list(header_sizes)
    for r in xrange(row_index, row_index+row_count):
        if r < len(rows):
            row = rows[r]
            for c in xrange(len(sizes)):
                value = row[c]
                entry = unicode(formatters[c](value))
                lines = entry.splitlines()
                max = 0
                for line in lines:
                    length = len(line)
                    if length > max:
                        max = length
                if max > sizes[c]:
                    sizes[c] = max
    return sizes


def _get_num_cols(schema, width, start_col_index, col_sizes, margin):
    """goes through the col_sizes starting at the given index and finds
       how many columns can be included on a line"""
    num_cols = 0
    line_length = margin - spaces_between_cols
    while line_length < width and start_col_index + num_cols < len(schema):
        candidate = col_sizes[start_col_index + num_cols] + spaces_between_cols
        if (line_length + candidate) > width:
            if num_cols == 0:
                num_cols = 1
            break
        num_cols += 1
        line_length += candidate

    return num_cols


def _get_row_clump_count(row_count, wrap):
    if row_count == 0:
        return 1
    return row_count / wrap + (1 if row_count % wrap else 0)


def _get_lines_from_extra_tuples(tuples, col_sizes, margin):
    # (for wrap formatting)
    # tuples is a list of tuples of the form (relative column index, [lines])
    # col_sizes is an array of the col_sizes for the 'current' clump (hence
    #   the 'relative column index' in the tuples list --these indices match
    #
    new_lines = []  # list of new, full-fledged extra lines that come from the tuples

    def there_are_tuples_in(x):
        return bool(len(x))

    while there_are_tuples_in(tuples):
        tuple_index = 0
        new_line_columns = [' ' * margin]
        for size_index in xrange(len(col_sizes)):
            if tuple_index < len(tuples) and size_index == tuples[tuple_index][0]:
                index, lines = tuples[tuple_index]  # the 'tuple'
                entry = lines.pop(0)
                new_line_columns.append(pad_right(entry, col_sizes[size_index]))
                if not len(lines):
                    del tuples[tuple_index]  # remove empty tuple, which also naturally moves index to the next tuple
                else:
                    tuple_index += 1  # move on to the next tuple
            else:
                new_line_columns.append(' ' * (col_sizes[size_index] + spaces_between_cols))

        new_lines.append(''.join(new_line_columns).rstrip())

    return new_lines


