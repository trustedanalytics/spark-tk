from sparktkregtests.lib.udftestlib import udf_remote_utils_indirect


def length(my_str):
    return udf_remote_utils_indirect.length(my_str)


def row_build(row):
    return length(row.letter)


def add_select_col(frame):

    import udf_remote_utils_indirect

    # When the interface is done, this will append utils_indirect.
    # ia.udf.install(['udf_remote_utils_indirect.py'])
    frame.add_columns(
        udf_remote_utils_indirect.selector, ('other_column', int))
