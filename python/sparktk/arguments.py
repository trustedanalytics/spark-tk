class implicit(object):
    """
    Type which acts a singleton value indicating that an argument should be implicitly filled

    Usage:

    def mult(a, b=2, base=implicit):
        if base is implicit:
            implicit.error('base')
        return base(a*b)
    """

    @staticmethod
    def error(arg_name):
        """Raises an error that the arg with the given name was found equal to implicit (i.e. its value was not provided, implicitly or explicitly)"""
        raise ValueError("Missing value for arg '%s'.  This value is normally filled implicitly, however, if this method is called standalone, it must be set explicitly" % arg_name)




def require_type(x, x_name, x_type, extra_msg=None):
    """
    raises a TypeError if the given x is not of type x_type, and accounts for implicits

    :param x:  the value
    :param x_name:  the name of the variable x for the error message
    :param x_type:  what type x is supposed to be

    Example
    -------

    >>> require_type(tc, 'tc', TkContext)

    """
    if x is implicit:
        implicit.error(x_name)
    if (x_type is not None and not isinstance(x, x_type)) or (x_type is None and x is not None):
        if extra_msg:
            extra_msg = '  ' + extra_msg
        raise TypeError("%s is of type %s.  Expected type %s.%s" % (x_name, type(x), x_type, extra_msg))
