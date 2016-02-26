
from sparktk import Context
global sc
c = Context(sc)

f = c.uploadFrame([[1, 2], [3, 4], [5, 6]])
print f.say()
print f.count()
