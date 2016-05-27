
def export_to_hive(self, hive_table_name):
    """
    Write current frame to Hive table.

    Table must not exist in Hive. Hive does not support case sensitive table names and columns names.
    Hence column names with uppercase letters will be converted to lower case by Hive.

    :param self: Current frame to export to hive table
    :param hive_table_name: hive table name

    Example
    --------
        <skip>

        >>> from sparktk import TkContext
        >>> tc=TkContext(sc)
        >>> data = [[1, 0.2, -2, 5], [2, 0.4, -1, 6], [3, 0.6, 0, 7], [4, 0.8, 1, 8]]
        >>> schema = [('a', int), ('b', float),('c', int) ,('d', int)]
        >>> my_frame = tc.to_frame(data, schema)
        <progress>

        table_name: (string): table name. It will create new table with given name if it does not exists already.

        >>> my_frame.export_to_hive("demo_test_hive")
        <progress>

        Verify exported frame in hive

        From bash shell

        $hive
        hive> show tables

        You should see demo_test_hive table.

        Run hive> select * from demo_test_hive; (to verify frame).

        </skip>
    """
    self._scala.exportToHive(hive_table_name)