
def compare_floats(a, b, precision=0.001):
    if len(a) != len(b):
        raise RuntimeError("sequences of floats have different lengths (%s != %s)\n  a: %s\n  b: %s" % (len(a), len(b), a, b))
    for i in xrange(len(a)):
        if abs(a[i] - b[i]) > precision:
            raise RuntimeError("Difference between floats %s and %s is greater than allowed precision %s\n  a: %s\n  b: %s" % (a[i], b[i], precision, a, b))


def test_x():
    raise Exception("Hey test runner, don't call me")