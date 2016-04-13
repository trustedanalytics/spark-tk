import os
import re
import sys
import inspect
import importlib
import logging
logger = logging.getLogger(__name__)


class LazyLoader(object):
    """
    Building-block object used to build out on-demand access to library subpackages, modules, and module contents.

    For a given file path, we build a custom class type (inheriting LazyLoader) which has properties to access each
    child subpackage or module found in the path.  These properties in turn, when accessed during runtime, create
    more lazy loader blocks for their descendents.  Only when module content is directly accessed does the module
    hierarchy actually get imported.
    """
    pass


def get_lazy_loader(instance, name, parent_path=None, package_name='sparktk'):
    """Gets the lazy loader for the given instance and relative descendent name"""
    validate_public_python_name(name)
    private_name = name_to_private(name)
    if private_name not in instance.__dict__:
        if parent_path is None:
            parent_path = os.path.dirname(os.path.abspath(__file__))  # default to local sparktk path, relative to here
        path = os.path.join(parent_path, name)
        lazy_loader = create_lazy_loader(path, package_name)
        setattr(instance, private_name, lazy_loader)
    return getattr(instance, private_name)


def create_lazy_loader(path, package_name):
    """Creates a lazy loader access object for the given path, usually an absolute path"""
    class_name = ''.join([piece.capitalize()
                          for piece in get_module_name(path, package_name).split('.')]) + LazyLoader.__name__
    lazy_loader_class = create_class_type(class_name, baseclass=LazyLoader)
    init_lazy_loader_class(lazy_loader_class, path, package_name)
    instance = lazy_loader_class()
    return instance


def init_lazy_loader_class(cls, path, package_name):
    """Initializes class (not instance!) by adding properties to access descendent subpackages, modules, and content"""

    # If path is a directory, then this lazy loader class needs properties for the descendents
    if os.path.isdir(path):
        children = os.listdir(path)
        for child_name in children:
            child_path = os.path.join(path, child_name)
            if os.path.isdir(child_path):
                add_loader_property(cls, child_name, child_path, package_name)
            elif os.path.isfile(child_path) and child_path.endswith('.py') and not child_name.startswith('_'):
                add_loader_property(cls, child_name[:-3], child_path, package_name)
            else:
                logger.info("LazyLoader skipping %s", child_path)

    # If path is a .py file, then this lazy loader class needs properties for the elements of the module
    elif os.path.isfile(path) and path.endswith('.py'):
        add_module_element_properties(cls, path, package_name)
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


def create_loader_property(name, path, package_name):
    """Creates a property whose getter will create, if it does not yet exist, a lazy loader object for the path"""
    loader_path = path
    private_name = name_to_private(name)
    loader_package_name = package_name

    def fget(self):
        if private_name not in self.__dict__:   # don't use hasattr, because it would match an inherited prop
            loader = create_lazy_loader(loader_path, loader_package_name)
            setattr(self, private_name, loader)
        return getattr(self, private_name)

    prop = property(fget=fget)
    return prop


def add_loader_property(cls, name, path, package_name):
    """Adds a property to the cls which accesses a lazy loader for a descendent"""
    prop = create_loader_property(name, path, package_name)
    logger.debug("Adding lazy loader property named %s to class %s for path %s", name, cls, path)
    setattr(cls, name, prop)


def add_module_element_properties(cls, path, package_name):
    """
    Dynamically imports the module and adds properties for each element found in the module to the given class

    __all__ is used if it is defined for the module, otherwise all non-private elements are aliased

    Module-level methods are aliased as static methods in the lazy loader class
    """
    module_name = get_module_name(path, package_name)
    logger.info("Dynamically loading module %s", module_name)
    m = importlib.import_module(module_name)
    if hasattr(m, '__all__'):
        d = dict([(k, m.__dict__(k)) for k in m.__all__])
    else:
        d = dict([(k, v) for k, v in m.__dict__.items() if not k.startswith('_')])

    for k, v in d.items():
        if hasattr(v, '__call__') and not inspect.isclass(v):
            v = staticmethod(v)
        logger.debug("Adding property %s for element %s to class %s for path %s", k, v, cls, path)
        setattr(cls, k, v)
