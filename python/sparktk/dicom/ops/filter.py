
def filter_by_keyword(self, list_of_keyword_value_tuple):
    """
    filter metadata with given list_of_keyword_value_tuple [("PatientID", "23212")]

    :param list_of_keyword_value_tuple (list[(str, str)]):
    :return: Frame with metadata and imagedata as columns
    """


def filter_by_tag(self, list_of_tag_value_tuple):
    """
    filter metadata with given list_of_tag_value_tuple

    list_of_tag_value_tuple([("00020001", "1.2.840.10008.5.1.4.1.1.4")])

    :param list_of_tag_value_tuple (list[(str, str)]): Tuple of columns and types
    :return:Frame with metadata and imagedata as columns
    """


def drop_by_keyword(self, list_of_keyword_value_tuple):
    """
    filter metadata which does not match with list_of_keyword_value_tuple [("PatientID", "23212")]

    :param list_of_keyword_value_tuple (list[(str, str)]):
    :return: Frame with metadata and imagedata as columns
    """


def drop_by_tag(self, list_of_tag_value_tuple):
    """
    filter metadata which does not match with given list_of_tag_value_tuple

    list_of_tag_value_tuple([("00020001", "1.2.840.10008.5.1.4.1.1.4")])

    :param list_of_tag_value_tuple (list[(str, str)]): Tuple of columns and types
    :return:Frame with metadata and imagedata as columns
    """


def filter(self, predicate):

    """
    Filter dicom using  given predicate

    :param self: dicom
    :param predicate: predicate to apply on filter

    #If it is on HDFS, path should be like hdfs://<ip-address>:8020/user/<user-name>/<dicom-directory-name>
        >>> dicom_path = "../datasets/dicom_uncompressed"

        >>> dicom = tc.dicom.import_dicom(dicom_path)

        <skip>
        >>> dicom.metadata.inspect(truncate=30)
        [#]  id  metadata
        =======================================
        [0]   0  <?xml version="1.0" encodin...
        [1]   1  <?xml version="1.0" encodin...
        [2]   2  <?xml version="1.0" encodin...
        </skip>

        >>> import xml.etree.ElementTree as ET

        #sample custom filter function
        >>> def filter_meta(tag_name, tag_value):
        ...    def _filter_meta(row):
        ...        root = ET.fromstring(row["metadata"])
        ...        for attribute in root.findall('DicomAttribute'):
        ...            keyword = attribute.get('keyword')
        ...            if attribute.get('keyword') is not None:
        ...                if attribute.find('Value') is not None:
        ...                    value = attribute.find('Value').text
        ...                    if keyword == tag_name and value == tag_value:
        ...                        return True
        ...    return _filter_meta

        >>> tag_name = "SOPInstanceUID"

        >>> tag_value = "1.3.12.2.1107.5.2.5.11090.5.0.5823667428974336"

        >>> dicom.filter(filter_meta(tag_name, tag_value))

        #After filter
        >>> dicom.metadata.inspect(truncate=30)
        [#]  id  metadata
        =======================================
        [0]   0  <?xml version="1.0" encodin...

    """

    self.metadata.filter(predicate)

