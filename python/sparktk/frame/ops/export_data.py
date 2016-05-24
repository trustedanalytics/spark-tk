
def export_to_hive(self, hive_table_name):
    """
    Write  current frame to Hive table.

    Table must not exist in Hive. Hive does not support case sensitive table names and columns names.
    Hence column names with uppercase letters will be converted to lower case by Hive.

    :param self: Current frame to export to hive table
    :param hive_table_name: hive table name

    """
    self._scala.exportToHive(hive_table_name)