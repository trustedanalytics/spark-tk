"""
   Support file for Remote UDF testing.
   Called from udf_remote_utils_direct.py,
     tests that multiple modules can invoke ia.udf.install()
"""

__author__ = "WDW"
__credits__ = ["Prune Wickart"]
__version__ = "2015.01.12"


def add_select_col(frame):

    from sparktkregtests.lib.udftestlib import udf_remote_utils_indirect

    # remote_path = __file__
    # remote_prefix = remote_path[0:remote_path.rfind('/')]

    # ia.udf.install([remote_prefix + '/udf_remote_utils_indirect.py'])
    frame.add_columns(
        udf_remote_utils_indirect.selector, ('other_column', int))
