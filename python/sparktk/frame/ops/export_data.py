
def export_to_json(self, path, count=4, offset=0):
    """
    Write current frame to HDFS in csv format.

    :param: path: The HDFS folder path where the files will be created.
    :param: count: The number of records you want. Default (0), or a non-positive value, is the whole frame.
    :param: offset: The number of rows to skip before exporting to the file. Default is zero (0).

    """
    self._scala.exportToJson(path, count, offset)
