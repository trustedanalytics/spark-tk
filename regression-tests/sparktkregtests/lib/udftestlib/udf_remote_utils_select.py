def add_select_col(frame):

    from sparktkregtests.lib.udftestlib import udf_remote_utils_indirect

    frame.add_columns(
        udf_remote_utils_indirect.selector, ('other_column', int))
