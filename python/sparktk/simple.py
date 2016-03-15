import json
from sparktk.inspect import pad_right


class SimpleObj(object):
    """Simple object which provides nice repr, to_dict, etc. for attributes and properties"""

    def to_dict(self):
        d = self._properties()
        d.update(self._attributes())
        return d

    def to_json(self):
        return json.dumps(self.to_dict())

    def __repr__(self):
        d = self.to_dict()
        max_len = 0
        for k in d.keys():
            max_len = max(max_len, len(k))
        return "\n".join(["%s = %s" % (pad_right(k, max_len), str(d[k])) for k in sorted(d.keys())])

    def _attributes(self):
        return dict([(k, v) for k, v in self.__dict__.items() if not k.startswith('_')])

    def _properties(self):
        class_items = self.__class__.__dict__.iteritems()
        return dict([(k, getattr(self, k)) for k, v in class_items if isinstance(v, property)])
