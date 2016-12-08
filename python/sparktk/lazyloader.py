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

import os
import re
import sys
import inspect
import importlib
import logging
from decorator import decorator
logger = logging.getLogger(__name__)

from sparktk.arguments import implicit


class LazyLoader(object):
    """
    Building-block object used to build out on-demand access to library subpackages, modules, and module contents.

    For a given file path, we build a custom class type (inheriting LazyLoader) which has properties to access each
    child subpackage or module found in the path.  These properties in turn, when accessed during runtime, create
    more lazy loader blocks for their descendents.  Only when module content is directly accessed does the module
    hierarchy actually get imported.

    Enables interactive script code, tabbing out the dots, like this:

    ``tc.models.clustering.kmeans.train(...)``

    Where nothing is actually loaded until the content of the kmeans module is needed.

    Also supports implicitly filling in function kwargs.  The lazy loader at creation time can be given a dict
    of kwargs which it will use to fill in function calls when referenced off the lazy loader.  For example, we can
    define the function:


        def great_read(uri, config, tc=implicit):
           return tc.thing.read(uri, config)

    If we lazily load great_read under tc somewhere, we'd like the tc to be passed implicitly, rather than
    awkwardly specifying it again:

        config = {'x': 1, 'y': 2}
        tc.readers.great_read('my_uri', config)  # no need to pass 'tc'

    """
    pass


def get_lazy_loader(instance, name, parent_path=None, package_name='sparktk', implicit_kwargs=None):
    """
    Gets the lazy loader for the given instance and relative descendent name

    :param instance: object which will own this lazy loader (the parent)
    :param name: the name the lazy loader will assume under this instance, which is the name of the module to be loaded
    :param parent_path: os path to the parent folder of the module called name; if not specified, it will be the path to THIS module's parent
    :param package_name: name of the root package within the parent_path
    :param implicit_kwargs:  dict of kwargs to use to implicitly fill arguments in functions loaded by this loader (and its descendents)
    :return: a lazy loader object
    """
    validate_public_python_name(name)
    private_name = name_to_private(name)
    logger.debug("get_lazy_loader(name=%s) --> private name=%s", name, private_name)
    if private_name not in instance.__dict__:
        if parent_path is None:
            parent_path = os.path.dirname(os.path.abspath(__file__))  # default to local sparktk path, relative to here
        path = os.path.join(parent_path, name)
        lazy_loader = create_lazy_loader(path, package_name, implicit_kwargs)
        setattr(instance, private_name, lazy_loader)
    return getattr(instance, private_name)


def create_lazy_loader(path, package_name, implicit_kwargs):
    """Creates a lazy loader access object for the given path, usually an absolute path"""
    class_name = ''.join([piece.capitalize()
                          for piece in get_module_name(path, package_name).split('.')]) + LazyLoader.__name__
    logger.debug("create_lazy_loader(path=%s, package_name=%s, implicit_kwargs=%s) --> class_name=%s", path, package_name, implicit_kwargs, class_name)
    lazy_loader_class = create_class_type(class_name, baseclass=LazyLoader)
    init_lazy_loader_class(lazy_loader_class, path, package_name, implicit_kwargs)
    instance = lazy_loader_class()
    return instance


def init_lazy_loader_class(cls, path, package_name, implicit_kwargs):
    """Initializes class (not instance!) by adding properties to access descendent subpackages, modules, and content"""

    # If path is a directory, then this lazy loader class needs properties for the descendents
    if os.path.isdir(path):
        children = os.listdir(path)
        for child_name in children:
            child_path = os.path.join(path, child_name)
            if child_name[0] != '_' and os.path.isdir(child_path):
                add_loader_property(cls, child_name, child_path, package_name, implicit_kwargs)
            elif child_name == "__init__.py" and os.path.isfile(child_path):
                # load any methods found in the (sub)package's __init__.py
                logger.debug("LazyLoader looking at __init__.py at %s (%s)", child_path, path)
                add_module_element_properties(cls, os.path.join(path, child_name), package_name, implicit_kwargs)
            elif child_name[0] != '_' and child_path.endswith('.py') and os.path.isfile(child_path):
                add_loader_property(cls, child_name[:-3], child_path, package_name, implicit_kwargs)
            else:
                logger.debug("LazyLoader skipping %s", child_path)

    # If path is a .py file, then this lazy loader class needs properties for the elements of the module
    elif os.path.isfile(path) and path.endswith('.py'):
        add_module_element_properties(cls, path, package_name, implicit_kwargs)
    else:
        raise ValueError("Bad path for LazyLoader init.  Expected valid package dir or .py file, but got %s" % path)


def create_class_type(new_class_name, baseclass):
    """Dynamically create a class type with the given name and namespace_obj"""
    logger.debug("Creating new class type '%s' with baseclass=%s", new_class_name, baseclass)
    new_class = type(str(new_class_name),
                     (baseclass,),
                     {'__module__': sys.modules[__name__]})
    # assign to its module, and to globals
    # http://stackoverflow.com/questions/13624603/python-how-to-register-dynamic-class-in-module
    setattr(sys.modules[new_class.__module__.__name__], new_class.__name__, new_class)
    globals()[new_class.__name__] = new_class
    return new_class


def get_module_name(path, package_name):
    """Determines the correct python module name for the given os path, relative to a particular package"""
    package_index = path.rfind(package_name)
    if package_index >= 0:
        path = path[package_index:]
    else:
        raise ValueError("package_name %s not found in path %s" % (package_name, path))
    if path.endswith('.py'):
        path = path[:-3]
    return path.replace('/', '.')


def name_to_private(name):
    """makes private version of the name"""
    return name if name.startswith('_') else '_' + name


def is_public_python_name(s):
    if s is None:
        raise ValueError("Expected string value, got None")
    return re.match('^[A-Za-z][\w_]*$', s) is not None


def validate_public_python_name(s):
    if not is_public_python_name(s):
        raise ValueError("Value %s is not a valid python variable name" % s)


def create_loader_property(name, path, package_name, implicit_kwargs):
    """Creates a property whose getter will create, if it does not yet exist, a lazy loader object for the path"""
    loader_path = path
    private_name = name_to_private(name)
    loader_package_name = package_name

    def fget(self):
        if private_name not in self.__dict__:   # don't use hasattr, because it would match an inherited prop
            loader = create_lazy_loader(loader_path, loader_package_name, implicit_kwargs)
            setattr(self, private_name, loader)
        return getattr(self, private_name)

    prop = property(fget=fget)
    return prop


def add_loader_property(cls, name, path, package_name, implicit_kwargs):
    """Adds a property to the cls which accesses a lazy loader for a descendent"""
    prop = create_loader_property(name, path, package_name, implicit_kwargs)
    logger.debug("Adding lazy loader property named %s to class %s for path %s", name, cls, path)
    setattr(cls, name, prop)


def add_module_element_properties(cls, path, package_name, implicit_kwargs):
    """
    Dynamically imports the module and adds properties for each element found in the module to the given class

    __all__ is used if it is defined for the module, otherwise all non-private elements are aliased

    Module-level methods are aliased as static methods in the lazy loader class
    """
    module_name = get_module_name(path, package_name)
    logger.info("Dynamically loading module %s", module_name)
    m = importlib.import_module(module_name)
    if hasattr(m, '__all__'):
        d = dict([(k, m.__dict__[k]) for k in m.__all__])
    else:
        d = dict([(k, v) for k, v in m.__dict__.items() if not k.startswith('_')])

    for k, v in d.items():
        if hasattr(v, '__call__') and not inspect.isclass(v):
            if implicit_kwargs:
                v = wrap_for_implicit_kwargs(v, implicit_kwargs)
            v = staticmethod(v)
        logger.debug("Adding property %s for element %s to class %s for path %s", k, v, cls, path)
        setattr(cls, k, v)


def wrap_for_implicit_kwargs(function, implicit_kwargs):
    """possibly wraps the function in a decorator which will implicitly fill in kwargs when called"""
    if not inspect.isfunction(function):
        logger.debug("wrap_for_implicit_kwargs(function=%s, implicit_kwargs=%s) 'function' arg is not a true function, so it is being returned untouched", function, implicit_kwargs)
        return function

    logger.debug("wrap_for_implicit_kwargs(function=%s, implicit_kwargs=%s)", function.__name__, implicit_kwargs)
    args, varargs, varkwargs, defaults = inspect.getargspec(function)
    logger.debug("argspec = (args=%s,varargs=%s,varkwargs=%s,defaults=%s)", args, varargs, varkwargs, defaults)
    kwarg_index_value_pairs = [(i, implicit_kwargs[key]) for i, key in enumerate(args)
                               if key in implicit_kwargs and validate_is_implicit(function.__name__, i, args, defaults)]
    if kwarg_index_value_pairs:
        def call_with_implicit_kwargs(func, *a, **kw):
            """call_with_implicit_kwargs wrapper used for a decorator"""
            args_list = list(a)
            for index, value in kwarg_index_value_pairs:
                if args_list[index] is implicit:
                    args_list[index]=value
            a = tuple(args_list)
            return func(*a, **kw)

        logger.debug("wrap_for_implicit_kwargs decorating %s with %s", function.__name__, kwarg_index_value_pairs)
        return decorator(call_with_implicit_kwargs, function)
    return function


def validate_is_implicit(function_name, arg_index, args, defaults):
    """Raises a TypeError if the kwarg does not have an implicit default value"""
    try:
        default_offset = len(args) - (len(defaults) if defaults else 0)
        default_index = arg_index - default_offset
        if default_index >= 0:
            assert(defaults[default_index] is implicit)
    except:
        raise TypeError("Lazyloader asked to implicitly fill arg '%s' but it is not marked implicit in function %s" %
                        (args[arg_index], function_name))
    return True  # return true for list comp construction
