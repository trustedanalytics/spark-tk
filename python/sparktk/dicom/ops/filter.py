def filter(self, predicate):

    """
    Filter dicom using  given predicate

    :param self: dicom
    :param predicate: predicate to apply on filter

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

        <skip>
        #After filter
        >>> dicom.metadata.inspect(truncate=30)
        [#]  id  metadata
        =======================================
        [0]   0  <?xml version="1.0" encodin...

        >>> dicom.pixeldata.inspect(truncate=30)
        [#]  id  imagematrix
        =========================================
        [0]   0  [[ 0.  0.  0. ...,  0.  0.  0.]
        [ 0.  7.  5. ...,  5.  7.  8.]
        [ 0.  7.  6. ...,  5.  6.  7.]
        ...,
        [ 0.  6.  7. ...,  5.  5.  6.]
        [ 0.  2.  5. ...,  5.  5.  4.]
        [ 1.  1.  3. ...,  1.  1.  0.]]
        </skip>

    """

    self.metadata.filter(predicate)
    filtered_id_frame = self.metadata.copy(columns = "id")
    self._pixeldata = filtered_id_frame.join_inner(self.pixeldata, "id")


def filter_by_keywords(self, dict_keywords_values):
    """
    filter metadata with given dictionary of keywords and values

    Ex: dict_keywords_values -> {"PatientID":"12344", "SOPInstanceUID","1.3.12.2.1107.5.2.5.11090.5.0.5823667428974336"}

    :param dict_keywords_values: (dict(str, str)) dictionary of keywords and values from xml string in metadata

    """


def filter_by_tags(self, dict_tags_values):
    """
    filter metadata with given dictionary of tags and values

     Ex: dict_tags_values -> {"00100020":"12344", "00080018","1.3.12.2.1107.5.2.5.11090.5.0.5823667428974336"}

    :param dict_tags_values: (dict(str, str)) dictionary of tags and values from xml string in metadata

    """


def drop_rows(self, predicate):

    """
    Drop rows of dicom metadata and pixeldata frames using  given predicate

    :param self: dicom
    :param predicate: predicate to apply on filter

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

        #sample custom filter function
        >>> def drop_meta(tag_name, tag_value):
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

        >>> dicom.drop_rows(drop_meta(tag_name, tag_value))

        <skip>
        #After filter
        >>> dicom.metadata.inspect(truncate=30)
        [#]  id  metadata
        =======================================
        [0]   1  <?xml version="1.0" encodin...
        [1]   2  <?xml version="1.0" encodin...

        >>> dicom.pixeldata.inspect(truncate=30)
        [#]  id  imagematrix
        =========================================
        [1]   1  [[  0.   1.   0. ...,   0.   0.   1.]
        [  1.   9.  10. ...,   2.   4.   6.]
        [  0.  12.  11. ...,   4.   4.   7.]
        ...,
        [  0.   4.   2. ...,   3.   5.   5.]
        [  0.   8.   5. ...,   7.   8.   8.]
        [  0.  10.  10. ...,   8.   8.   8.]]
        [2]   2  [[ 0.  0.  0. ...,  0.  0.  0.]
        [ 0.  2.  2. ...,  6.  5.  5.]
        [ 0.  7.  8. ...,  4.  4.  5.]
        ...,
        [ 0.  4.  1. ...,  4.  5.  6.]
        [ 0.  4.  5. ...,  6.  6.  5.]
        [ 1.  6.  8. ...,  4.  5.  4.]]
        </skip>

    """

    def inverted_predicate(row):
        return not predicate(row)

    self.metadata.filter(inverted_predicate)
    filtered_id_frame = self.metadata.copy(columns= "id")
    self._pixeldata = filtered_id_frame.join_inner(self.pixeldata, "id")


def drop_rows_by_keywords(self, dict_keywords_values):
    """
    filter metadata which does not match with given dictionary of keywords and values

    Ex: dict_keywords_values -> {"PatientID":"12344", "SOPInstanceUID","1.3.12.2.1107.5.2.5.11090.5.0.5823667428974336"}

    :param dict_keywords_values: (dict(str, str)) dictionary of keywords and values from xml string in metadata

    """


def drop_rows_by_tags(self, dict_tags_values):
    """
    filter metadata which does not match with given dictionary of tags and values

     Ex: dict_tags_values -> {"00100020":"12344", "00080018","1.3.12.2.1107.5.2.5.11090.5.0.5823667428974336"}

    :param dict_tags_values: (dict(str, str)) dictionary of tags and values from xml string in metadata

    """


