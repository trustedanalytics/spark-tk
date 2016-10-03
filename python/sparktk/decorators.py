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


def alpha(item):
    item.maturity = 'alpha'
    item.__doc__ = "(Note: 'alpha' maturity)\n" + item.__doc__
    return item


def beta(item):
    item.maturity = 'beta'
    item.__doc__ = "(Note: 'beta' maturity)\n" + item.__doc__
    return item


def deprecated(item):
    """decorator for deprecation; if item is a string, then it is used as a message"""
    from decorator import decorator
    if isinstance(item, basestring):
        message = item
    else:
        message = ''

    def deprecated_item(it):
        def wrapper(x, *args, **kwargs):
            raise_deprecation_warning(x.__name__, message)
            return x(*args, **kwargs)
        function = decorator(wrapper, it)
        function.maturity = 'deprecated'
        return function

    return deprecated_item if message else deprecated_item(item)


def raise_deprecation_warning(function_name, message=''):
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter('default')  # make it so Python 2.7 will still report this warning
        m = "Call to deprecated function %s." % function_name
        if message:
            m += "  %s" % message
        warnings.warn(m, DeprecationWarning, stacklevel=2)

