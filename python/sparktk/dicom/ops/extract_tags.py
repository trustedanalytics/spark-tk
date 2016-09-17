def extract_tags(self, tags):
    """
    Extract value for each tag from column holding xml string

    Ex: tags -> ["00020001", "00020002"]

    Parameters
    ----------

    :param tags: (str or list(str)) List of tags from xml string of metadata column


    Examples
    --------

        <skip>
        >>> dicom_path = "../datasets/dicom_uncompressed"

        >>> dicom = tc.dicom.import_dcm(dicom_path)

        >>> dicom.metadata.inspect(truncate=30)
        [#]  id  metadata
        =======================================
        [0]   0  <?xml version="1.0" encodin...
        [1]   1  <?xml version="1.0" encodin...
        [2]   2  <?xml version="1.0" encodin...

        #Extract value for each tag from column holding xml string
        >>> dicom.extract_tags(["00080018", "00080070", "00080030"])

        >>> dicom.metadata.inspect(truncate=20)
        [#]  id  metadata              00080018              00080070  00080030
        ============================================================================
        [0]   0  <?xml version="1....  1.3.12.2.1107.5.2...  SIEMENS   085922.859000
        [1]   1  <?xml version="1....  1.3.12.2.1107.5.2...  SIEMENS   085922.859000
        [2]   2  <?xml version="1....  1.3.12.2.1107.5.2...  SIEMENS   085922.859000
        </skip>

    """

    if isinstance(tags, basestring):
        tags = [tags]

    if self._metadata._is_scala:
        def f(scala_dicom):
            scala_dicom.extractTags(self._tc.jutils.convert.to_scala_vector_string(tags))
        results = self._call_scala(f)
        return results

    # metadata is python frame, run below udf
    import xml.etree.ElementTree as ET

    def extractor(dtags):
        def _extractor(row):
            root = ET.fromstring(row["metadata"])
            values=[None]*len(dtags)
            for attribute in root.findall('DicomAttribute'):
                dtag = attribute.get('tag')
                if dtag in dtags:
                    if attribute.find('Value') is not None:
                        values[dtags.index(dtag)]=attribute.find('Value').text
            return values
        return _extractor

    cols_type = [(tag, str) for tag in tags]
    self._metadata.add_columns(extractor(tags), cols_type)