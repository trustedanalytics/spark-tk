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
Type checking for arguments, including the implicit argument
"""

__all__ = ['implicit', 'affirm_type', 'require_type', 'validate_call']


class implicit(object):
    """
    Type which acts a singleton value indicating that an argument should be implicitly filled

    Usage:

    def mult(a, b=2, base=implicit):
        if base is implicit:
            implicit.error('base')
        return base(a*b)
    """

    @staticmethod
    def error(arg_name):
        """Raises an error that the arg with the given name was found equal to implicit (i.e. its value was not provided, implicitly or explicitly)"""
        raise ValueError("Missing value for arg '%s'.  This value is normally filled implicitly, however, if this method is called standalone, it must be set explicitly" % arg_name)


def type_error(expected_type, x_type, x_name, extra_msg=None):
    """returns a commonly formatted type error which can then be raised by individual methods"""
    extra_msg = '' if extra_msg is None else '  ' + extra_msg
    return TypeError("Value for %s is of type %s.  Expected type %s.%s" % (x_name, x_type, expected_type, extra_msg))


def value_error(expected_value_description, x_value, x_name, extra_msg=None):
    """returns a commonly formatted value error which can then be raised by individual methods"""
    extra_msg = '' if extra_msg is None else '  ' + extra_msg
    return ValueError("Found %s = %s.  Expected %s.%s" % (x_name, x_value, expected_value_description, extra_msg))


class _AffirmType(object):
    """
    Holds methods which affirm the value is of a particular type, returning it as that type after possible conversion

    Raises ValueError otherwise
    """

    def list_of_str(self, value, name, extra_msg=None, length=None):
        """Note: converts str to list of str"""
        if isinstance(value, basestring):
            return [value]
        if length is not None and len(value) != length:
            raise value_error("list of str of length %s" % length, value, name, extra_msg)
        if not isinstance(value, list):
            raise type_error("str or list of str", type(value), name, extra_msg)
        if not all(isinstance(c, basestring) for c in value):
            raise value_error("str or list of str", value, name, extra_msg)
        return value

    def list_of_float(self, value, name, extra_msg=None, length=None):
        values = value if isinstance(value, list) else [value]
        if length is not None and len(values) != length:
            raise value_error("list of float of length %s" % length, value, name, extra_msg)
        try:
            x = [float(f) for f in values]
        except ValueError:
            raise value_error("list of float", value, name, extra_msg)
        return x


affirm_type = _AffirmType()  # singleton instance of the _AffirmType class


class _RequireType(object):
    """
    raises a TypeError or ValueError if the given x_value not does meet the requirements (accounts for implicit)

    :param required_type:  (type) what type x is supposed to be
    :param value:  the value in question
    :param name:  (str) the name of the variable for the possible error message
    :param extra_msg:  (Optional) extra message text for the possible error

    Example
    -------

    >>> a = 1
    >>> require_type(int, a, 'a')

    """
    def __call__(self, required_type, value, name, extra_msg=None):
        if value is implicit:
            implicit.error(name)
        if (required_type is not None and not isinstance(value, required_type))\
                or (required_type is None and value is not None):
            raise type_error(required_type, type(value), name, extra_msg)

    # **please update unit tests (test_arguments.py) if you add methods to this class

    def non_empty_str(self, value, name, extra_msg=None):
        if not isinstance(value, basestring):
            raise type_error(str, type(value), name, extra_msg)
        if not value:
            raise value_error("non-empty string", value, name, extra_msg)

    def non_negative_int(self, value, name, extra_msg=None):
        if not isinstance(value, int):
            raise type_error(int, type(value), name, extra_msg)
        if value < 0:
            raise value_error("non-negative integer", value, name, extra_msg)



require_type = _RequireType()  #singleton instance of the _RequireType class


def is_name_private(name, public=None):
    """
    Answers whether the name is considered private, by checking the leading underscore.

    A ist of public names can be provided which would override the normal check
    """
    if public and name in public:
        return False
    return name.startswith('_')


def default_value_to_str(value):
    """renders value to be printed in a function signature"""
    return value if value is None or type(value) not in [str, unicode] else "'%s'" % value


def get_args_spec_from_function(function, ignore_self=False, ignore_private_args=False):
    """
    Returns the spec of the arguments as a tuple

    :param function:  the function to interrogate
    :param ignore_self:  whether to ignore a 'self' argument
    :param ignore_private_args: whether to ignore arguments which start with an '_'
    :return: tuple of args, kwargs, varargs, varkwargs
        args - list of strings the names of the required arguments.  None if not defined
        kwargs - list of tuples of (name, default value).  None if not defined
        varargs - name of a var args (e.g. "args" for "*args).   None if not defined
        varkwargs - name of a var kwargs (e.g. "kwargs" for "**kwargs).  None if not defined
    """
    import inspect

    args, varargs, varkwargs, defaults = inspect.getargspec(function)
    if ignore_self:
        args = [a for a in args if a != 'self']

    num_defaults = len(defaults) if defaults else 0
    if num_defaults:
        kwargs = zip(args[-num_defaults:], defaults)
        args = args[:-num_defaults]
    else:
        kwargs = []

    if ignore_private_args:
        args = [name for name in args if not is_name_private(name)]
        kwargs = [(name, value) for name, value in kwargs if not is_name_private(name)]
    return args, kwargs, varargs, varkwargs  # todo - make nametuple


def get_args_text_from_function(function, ignore_self=False, ignore_private_args=False):
    """Returns a string which is the familiar python representation of the function's argument signature"""
    args, kwargs, varargs, varkwargs = get_args_spec_from_function(function, ignore_self, ignore_private_args)
    args.extend(["%s=%s" % (name, default_value_to_str(value)) for name, value in kwargs])
    if varargs:
        args.append('*' + varargs)
    if varkwargs:
        args.append('**' + varkwargs)
    text = ", ".join(args)
    return text


def validate_call(function, arguments):
    """Validates the a dict of arguments can be used to call the given function"""

    require_type(dict, arguments, "arguments")
    args, kwargs, varargs, varkwargs = get_args_spec_from_function(function)
    if varargs:
        signature = get_args_text_from_function(function)
        raise ValueError("function %s(%s) cannot be validated against a dict of parameters because of the '*%s' in its signature"
                         % (function.__name__, signature, varargs))

    missing_args = list(args)
    invalid_args = []

    valid_kwarg_names = [name for name, default_val in kwargs]

    for name in arguments:
        if name in args:
            missing_args.remove(name)
        elif name not in valid_kwarg_names:
            if not varkwargs:
                invalid_args.append(name)

    if missing_args or invalid_args:
        signature = get_args_text_from_function(function)
        # todo - add Levenshtein distance calc's and helpful messages
        if invalid_args:
            raise ValueError("call to function %s(%s) included one or more unknown args named: %s" % (function.__name__, signature, ', '.join(invalid_args)))
        else:
            raise ValueError("call to function %s(%s) is missing the following required arguments: %s" % (function.__name__, signature, ', '.join(missing_args)))


