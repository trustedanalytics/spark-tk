
def add_columns_by_keyword(self, list_of_keywords):
    """
    adds given list_of_keywords as column and stores the value for each tag as the column value in metadata frame

    :param list_of_keywords (str or list(str)):

    """


def add_columns_by_tag(self, list_of_tags):
    """
    adds given list_of_tags as column and stores the value for each tag as the column value in metadata frame

    list_of_tags["00020001", "00020002"]

    :param list_of_tags (str or list(str)): Tuple of columns and types

    """


def add_columns(self, func, schema):

    """
    add columns to dicom using  given function

    :param self: dicom
    :param func: function to apply on row
    :param schema: schema for the column(s) to be added

    Examples:
    ---------

        >>> dicom_path = "../datasets/dicom_uncompressed"

        >>> dicom = tc.dicom.import_dcm(dicom_path)

        <skip>
        >>> dicom.metadata.inspect(truncate=30)
        [#]  id  metadata
        =======================================
        [0]   0  <?xml version="1.0" encodin...
        [1]   1  <?xml version="1.0" encodin...
        [2]   2  <?xml version="1.0" encodin...
        </skip>

        >>> import xml.etree.ElementTree as ET

        #sample function to apply on row
        >>> def extractor(tag_name):
        ...    def _extractor(row):
        ...        root = ET.fromstring(row["metadata"])
        ...        for attribute in root.findall('DicomAttribute'):
        ...            keyword = attribute.get('keyword')
        ...            value = None
        ...            if attribute.find('Value') is not None:
        ...                value = attribute.find('Value').text
        ...            if keyword == tag_name:
        ...                return value
        ...    return _extractor

        >>> tag_name = "SOPInstanceUID"

        >>> dicom.metadata.add_columns(extractor(tag_name), (tag_name, str))

        <skip>
        >>> dicom.metadata.inspect(truncate=30)
        [#]  id  metadata                        SOPInstanceUID
        =======================================================================
        [0]   0  <?xml version="1.0" encodin...  1.3.12.2.1107.5.2.5.11090.5...
        [1]   1  <?xml version="1.0" encodin...  1.3.12.2.1107.5.2.5.11090.5...
        [2]   2  <?xml version="1.0" encodin...  1.3.12.2.1107.5.2.5.11090.5...
        </skip>

    """
    self.metadata.add_columns(func, schema)