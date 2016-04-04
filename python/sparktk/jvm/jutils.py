from py4j import java_gateway as _py4j_gateway
from jconvert import JConvert

class JUtils(object):
    def __init__(self, sc):
        self.sc = sc
        self.convert = JConvert(self)

    @staticmethod
    def is_java(item):
        return isinstance(item, _py4j_gateway.JavaObject)

    def is_jvm_instance_of(self, item, scala_type):
        if self.is_java(item):
            return _py4j_gateway.is_instance_of(self.sc._gateway, item, scala_type)
        return False

    def jhelp(self, item):
        """shortcut to py4j's help method"""
        self.sc._gateway.help(item)

    @staticmethod
    def jtypestr(item):
        """string representation of the item's Java Type"""
        if JUtils.is_java(item):
            return item.getClass().getName()
        return "<Not a JVM Object>"