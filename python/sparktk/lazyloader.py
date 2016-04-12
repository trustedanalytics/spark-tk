import os
import sys
import inspect
import importlib
import logging
logger = logging.getLogger("sparktk")


class LazyLoader(object):
    """aliased lazy loader for library"""
    pass


def create_lazy_loader(path):
    class_name = ''.join([piece.capitalize() for piece in get_module_name(path).split('.')]) + LazyLoader.__name__
    lazy_loader_class = create_class_type(class_name, LazyLoader)
    init_loader_class(lazy_loader_class, path)
    return lazy_loader_class()


def get_module_name(path):
    index_of_sparktk = path.rfind('sparktk')
    if index_of_sparktk >= 0:
        path = path[index_of_sparktk:]
    if path.endswith('.py'):
        path = path[:-3]
    return path.replace('/', '.')


def init_loader_class(cls, path):
    # print "Initializing LazyLoader, path = %s" % path
    if os.path.isdir(path):
        print "*****isdir"
        children = os.listdir(path)
        print "*****children=%s" % children
        for child_name in children:
            child_path = os.path.join(path, child_name)
            # identify child dirs
            # print "*****child_path=%s" % child_path
            if os.path.isdir(child_path):
                add_loader_property(cls, child_name, child_path)
            elif os.path.isfile(child_path) and child_path.endswith('.py') and child_name != '__init__.py':
                child_name = child_name[:-3]
                add_loader_property(cls, child_name, child_path)
                # foreach child dir, create a property which returns a new lazy loader for that dir
                # foreach child .py file, create a property which returns a lazy loader for that file
                # self.add_loader_property(child_path)
            else:
                logger.info("LazyLoader skipping %s", child_path)

    # if path is .py file:
    elif os.path.isfile(path) and path.endswith('.py'):
        # print "*****is py file"
        # foreach element in the .py file __all__ create a property which returns that element.
        add_file_element_properties(cls, path)
    else:
        # else: Error!
        raise RuntimeError("Problem in LazyLoader build out... path=%s" % path)


def create_class_type(new_class_name, baseclass):
    """Dynamically create a class type with the given name and namespace_obj"""
    if logger.level == logging.DEBUG:
        logger.debug("create_class_type(new_class_name='%s', baseclass=%s)",
                     new_class_name,
                     baseclass)
    new_class = type(str(new_class_name),
                     (baseclass,),
                     {'__module__': sys.modules[__name__]})
    # assign to its module, and to globals
    # http://stackoverflow.com/questions/13624603/python-how-to-register-dynamic-class-in-module
    setattr(sys.modules[new_class.__module__.__name__], new_class.__name__, new_class)
    globals()[new_class.__name__] = new_class
    return new_class


def name_to_private(name):
    """makes private version of the name"""
    return name if name.startswith('_') else '_' + name


def add_loader_property(cls, name, path):
    prop = create_loader_property(name, path)
    # print "** Adding property named %s" % name
    setattr(cls, name, prop)

def get_index_of_sparktk(path):
    index_of_sparktk = path.rfind('sparktk')
    if index_of_sparktk < 0:
        raise RuntimeError("LazyLoader could not find sparktk in path %s" % path)
    return index_of_sparktk

def add_file_element_properties(cls, path):
    # print "**  _add_file_element_properties path=%s" % path
    module_name = get_module_name(path)
    # print "** _add_file_element_properties module_name=%s" % module_name
    m = importlib.import_module(module_name)
    if hasattr(m, '__all__'):
        d = dict([(k, m.__dict__(k)) for k in m.__all__])
    else:
        d = dict([(k, v) for k, v in m.__dict__.items() if not k.startswith('_')])

    for k, v in d.items():
        if hasattr(v, '__call__') and not inspect.isclass(v):
            v = staticmethod(v)
        setattr(cls, k, v)


def create_loader_property(name, path):
    loader_path = path
    private_name = name_to_private(name)

    def fget(self):
        if private_name not in self.__dict__:   # don't use hasattr, because it would match an inherited prop
            loader = create_lazy_loader(loader_path)
            setattr(self, private_name, loader)
        return getattr(self, private_name)

    prop = property(fget=fget)  # , doc=doc)
    return prop