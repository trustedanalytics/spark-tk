def filter(self, predicate):

    """
    Filter dicom using  given predicate

    Parameters
    ----------

    :param predicate: predicate to apply on filter


    Examples
    --------

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
