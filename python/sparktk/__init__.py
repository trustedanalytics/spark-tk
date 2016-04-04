from tkcontext import TkContext, create_sc
tc = TkContext()
del TkContext

import dtypes
from sparktk.loggers import loggers
from sparktk.frame.ops.inspect import inspect_settings
from sparktk.frame.frame import to_frame, load_frame

def init(sc):
    tc.init(sc)

