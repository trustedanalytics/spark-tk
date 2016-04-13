print "** Loading pso.py"

def pso(**kwargs):
    return PsoModel(**kwargs)


class PsoModel(object):

    def __init__(self, **kwargs):
        self.kw = kwargs

    def __repr__(self):
        return str(self.kw)
